using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace CustomDatabase
{
    // Data Model
    public class CowModel
    {
        public Guid Id { get; set; }
        public string Breed { get; set; }
        public int Age { get; set; }
        public string Name { get; set; }
        public byte[] DnaData { get; set; }
    }

    // Database Interface
    public interface ICowDatabase
    {
        void Insert(CowModel cow);
        void Delete(CowModel cow);
        void Update(CowModel cow);
        CowModel Find(Guid id);
        IEnumerable<CowModel> FindBy(string breed, int age);
    }

    // Block Storage Interfaces
    public interface IBlockStorage
    {
        int BlockContentSize { get; }
        int BlockHeaderSize { get; }
        int BlockSize { get; }
        IBlock Find(uint blockId);
        IBlock CreateNew();
    }

    public interface IBlock : IDisposable
    {
        uint Id { get; }
        long GetHeader(int field);
        void SetHeader(int field, long value);
        void Read(byte[] dst, int dstOffset, int srcOffset, int count);
        void Write(byte[] src, int srcOffset, int dstOffset, int count);
    }

    // Block Storage Implementation
    public class BlockStorage : IBlockStorage
    {
        private readonly Stream _stream;
        private readonly int _blockSize;
        private readonly int _headerSize;

        public BlockStorage(Stream stream, int blockSize = 4096, int headerSize = 48)
        {
            _stream = stream;
            _blockSize = blockSize;
            _headerSize = headerSize;
        }

        public int BlockContentSize => _blockSize - _headerSize;
        public int BlockHeaderSize => _headerSize;
        public int BlockSize => _blockSize;

        public IBlock Find(uint blockId)
        {
            return new Block(this, _stream, blockId, _blockSize, _headerSize);
        }

        public IBlock CreateNew()
        {
            var blockId = (uint)(_stream.Length / _blockSize);
            _stream.SetLength((_stream.Length / _blockSize + 1) * _blockSize);
            return new Block(this, _stream, blockId, _blockSize, _headerSize);
        }
    }

    public class Block : IBlock
    {
        private readonly BlockStorage _storage;
        private readonly Stream _stream;
        private readonly uint _id;
        private readonly int _blockSize;
        private readonly int _headerSize;
        private readonly byte[] _cache;
        private bool _isDirty;

        public Block(BlockStorage storage, Stream stream, uint id, int blockSize, int headerSize)
        {
            _storage = storage;
            _stream = stream;
            _id = id;
            _blockSize = blockSize;
            _headerSize = headerSize;
            _cache = new byte[_blockSize];
            LoadCache();
        }

        public uint Id => _id;

        private void LoadCache()
        {
            _stream.Position = _id * _blockSize;
            _stream.Read(_cache, 0, _blockSize);
        }

        public long GetHeader(int field)
        {
            if (field < 0 || field >= _headerSize / 8) throw new ArgumentOutOfRangeException();
            return BitConverter.ToInt64(_cache, field * 8);
        }

        public void SetHeader(int field, long value)
        {
            if (field < 0 || field >= _headerSize / 8) throw new ArgumentOutOfRangeException();
            BitConverter.GetBytes(value).CopyTo(_cache, field * 8);
            _isDirty = true;
        }

        public void Read(byte[] dst, int dstOffset, int srcOffset, int count)
        {
            Array.Copy(_cache, _headerSize + srcOffset, dst, dstOffset, count);
        }

        public void Write(byte[] src, int srcOffset, int dstOffset, int count)
        {
            Array.Copy(src, srcOffset, _cache, _headerSize + dstOffset, count);
            _isDirty = true;
        }

        public void Dispose()
        {
            if (_isDirty)
            {
                _stream.Position = _id * _blockSize;
                _stream.Write(_cache, 0, _blockSize);
                _isDirty = false;
            }
        }
    }

    // Record Storage Interface
    public interface IRecordStorage
    {
        void Update(uint recordId, byte[] data);
        byte[] Find(uint recordId);
        uint Create();
        uint Create(byte[] data);
        uint Create(Func<uint, byte[]> dataGenerator);
        void Delete(uint recordId);
    }

    // Record Storage Implementation
    public class RecordStorage : IRecordStorage
    {
        private readonly IBlockStorage _blockStorage;
        private const int NextBlockId = 0;
        private const int BlockContentLength = 2;
        private const int PreviousBlockId = 3;
        private const int IsDeleted = 4;

        public RecordStorage(IBlockStorage blockStorage)
        {
            _blockStorage = blockStorage;
            InitializeDeletedBlocksRecord();
        }

        private void InitializeDeletedBlocksRecord()
        {
            try
            {
                _blockStorage.Find(0);
            }
            catch
            {
                _blockStorage.CreateNew(); // Create record #0 for deleted blocks
            }
        }

        public uint Create()
        {
            return GetOrCreateBlock();
        }

        public uint Create(byte[] data)
        {
            var recordId = Create();
            Update(recordId, data);
            return recordId;
        }

        public uint Create(Func<uint, byte[]> dataGenerator)
        {
            var recordId = Create();
            var data = dataGenerator(recordId);
            Update(recordId, data);
            return recordId;
        }

        public void Update(uint recordId, byte[] data)
        {
            var blocks = GetRecordBlocks(recordId);
            var dataOffset = 0;
            var blockIndex = 0;

            while (dataOffset < data.Length)
            {
                IBlock block;
                if (blockIndex < blocks.Count)
                {
                    block = blocks[blockIndex];
                }
                else
                {
                    var newBlockId = GetOrCreateBlock();
                    block = _blockStorage.Find(newBlockId);
                    if (blocks.Count > 0)
                    {
                        blocks.Last().SetHeader(NextBlockId, newBlockId);
                        blocks.Last().Dispose();
                    }
                    blocks.Add(block);
                }

                var chunkSize = Math.Min(_blockStorage.BlockContentSize, data.Length - dataOffset);
                block.Write(data, dataOffset, 0, chunkSize);
                block.SetHeader(BlockContentLength, chunkSize);
                
                if (blockIndex > 0)
                    block.SetHeader(PreviousBlockId, blocks[blockIndex - 1].Id);
                
                dataOffset += chunkSize;
                blockIndex++;
            }

            // Mark extra blocks as deleted
            while (blockIndex < blocks.Count)
            {
                MarkBlockAsDeleted(blocks[blockIndex].Id);
                blocks.RemoveAt(blockIndex);
            }

            foreach (var block in blocks)
                block.Dispose();
        }

        public byte[] Find(uint recordId)
        {
            var blocks = GetRecordBlocks(recordId);
            if (blocks.Count == 0) return null;

            var totalSize = blocks.Sum(b => (int)b.GetHeader(BlockContentLength));
            var result = new byte[totalSize];
            var offset = 0;

            foreach (var block in blocks)
            {
                var contentLength = (int)block.GetHeader(BlockContentLength);
                block.Read(result, offset, 0, contentLength);
                offset += contentLength;
                block.Dispose();
            }

            return result;
        }

        public void Delete(uint recordId)
        {
            var blocks = GetRecordBlocks(recordId);
            foreach (var block in blocks)
            {
                MarkBlockAsDeleted(block.Id);
                block.Dispose();
            }
        }

        private List<IBlock> GetRecordBlocks(uint recordId)
        {
            var blocks = new List<IBlock>();
            var currentBlockId = recordId;

            while (currentBlockId != 0)
            {
                var block = _blockStorage.Find(currentBlockId);
                if (block.GetHeader(IsDeleted) != 0)
                    break;

                blocks.Add(block);
                currentBlockId = (uint)block.GetHeader(NextBlockId);
            }

            return blocks;
        }

        private uint GetOrCreateBlock()
        {
            // Try to reuse a deleted block
            var deletedBlocksRecord = _blockStorage.Find(0);
            var stackSize = (int)deletedBlocksRecord.GetHeader(BlockContentLength);
            
            if (stackSize > 0)
            {
                var stackData = new byte[stackSize];
                deletedBlocksRecord.Read(stackData, 0, 0, stackSize);
                
                if (stackSize >= 4)
                {
                    var reusedBlockId = BitConverter.ToUInt32(stackData, stackSize - 4);
                    var newStackData = new byte[stackSize - 4];
                    Array.Copy(stackData, 0, newStackData, 0, stackSize - 4);
                    
                    deletedBlocksRecord.Write(newStackData, 0, 0, newStackData.Length);
                    deletedBlocksRecord.SetHeader(BlockContentLength, newStackData.Length);
                    deletedBlocksRecord.Dispose();
                    
                    return reusedBlockId;
                }
            }
            
            deletedBlocksRecord.Dispose();
            return _blockStorage.CreateNew().Id;
        }

        private void MarkBlockAsDeleted(uint blockId)
        {
            var block = _blockStorage.Find(blockId);
            block.SetHeader(IsDeleted, 1);
            block.Dispose();

            // Add to deleted blocks stack
            var deletedBlocksRecord = _blockStorage.Find(0);
            var stackSize = (int)deletedBlocksRecord.GetHeader(BlockContentLength);
            var stackData = new byte[stackSize + 4];
            
            if (stackSize > 0)
            {
                deletedBlocksRecord.Read(stackData, 0, 0, stackSize);
            }
            
            BitConverter.GetBytes(blockId).CopyTo(stackData, stackSize);
            deletedBlocksRecord.Write(stackData, 0, 0, stackData.Length);
            deletedBlocksRecord.SetHeader(BlockContentLength, stackData.Length);
            deletedBlocksRecord.Dispose();
        }
    }

    // Simple In-Memory Index Implementation (B-Tree would be more complex)
    public interface IIndex<K, V>
    {
        void Insert(K key, V value);
        Tuple<K, V> Get(K key);
        IEnumerable<Tuple<K, V>> LargerThanOrEqualTo(K key);
        IEnumerable<Tuple<K, V>> LargerThan(K key);
        IEnumerable<Tuple<K, V>> LessThanOrEqualTo(K key);
        IEnumerable<Tuple<K, V>> LessThan(K key);
        bool Delete(K key, V value, IComparer<V> valueComparer = null);
        bool Delete(K key);
    }

    public class MemoryIndex<K, V> : IIndex<K, V> where K : IComparable<K>
    {
        private readonly SortedDictionary<K, List<V>> _index = new SortedDictionary<K, List<V>>();

        public void Insert(K key, V value)
        {
            if (!_index.ContainsKey(key))
                _index[key] = new List<V>();
            _index[key].Add(value);
        }

        public Tuple<K, V> Get(K key)
        {
            if (_index.ContainsKey(key) && _index[key].Count > 0)
                return new Tuple<K, V>(key, _index[key][0]);
            return null;
        }

        public IEnumerable<Tuple<K, V>> LargerThanOrEqualTo(K key)
        {
            return _index.Where(kvp => kvp.Key.CompareTo(key) >= 0)
                         .SelectMany(kvp => kvp.Value.Select(v => new Tuple<K, V>(kvp.Key, v)));
        }

        public IEnumerable<Tuple<K, V>> LargerThan(K key)
        {
            return _index.Where(kvp => kvp.Key.CompareTo(key) > 0)
                         .SelectMany(kvp => kvp.Value.Select(v => new Tuple<K, V>(kvp.Key, v)));
        }

        public IEnumerable<Tuple<K, V>> LessThanOrEqualTo(K key)
        {
            return _index.Where(kvp => kvp.Key.CompareTo(key) <= 0)
                         .SelectMany(kvp => kvp.Value.Select(v => new Tuple<K, V>(kvp.Key, v)));
        }

        public IEnumerable<Tuple<K, V>> LessThan(K key)
        {
            return _index.Where(kvp => kvp.Key.CompareTo(key) < 0)
                         .SelectMany(kvp => kvp.Value.Select(v => new Tuple<K, V>(kvp.Key, v)));
        }

        public bool Delete(K key, V value, IComparer<V> valueComparer = null)
        {
            if (!_index.ContainsKey(key)) return false;
            
            var comparer = valueComparer ?? Comparer<V>.Default;
            var removed = _index[key].RemoveAll(v => comparer.Compare(v, value) == 0) > 0;
            
            if (_index[key].Count == 0)
                _index.Remove(key);
                
            return removed;
        }

        public bool Delete(K key)
        {
            return _index.Remove(key);
        }
    }

    // Cow Serializer
    public static class CowSerializer
    {
        public static byte[] Serialize(CowModel cow)
        {
            using (var ms = new MemoryStream())
            using (var writer = new BinaryWriter(ms))
            {
                writer.Write(cow.Id.ToByteArray());
                writer.Write(cow.Breed ?? "");
                writer.Write(cow.Age);
                writer.Write(cow.Name ?? "");
                writer.Write(cow.DnaData?.Length ?? 0);
                if (cow.DnaData != null)
                    writer.Write(cow.DnaData);
                return ms.ToArray();
            }
        }

        public static CowModel Deserialize(byte[] data)
        {
            using (var ms = new MemoryStream(data))
            using (var reader = new BinaryReader(ms))
            {
                var cow = new CowModel();
                cow.Id = new Guid(reader.ReadBytes(16));
                cow.Breed = reader.ReadString();
                cow.Age = reader.ReadInt32();
                cow.Name = reader.ReadString();
                var dnaLength = reader.ReadInt32();
                if (dnaLength > 0)
                    cow.DnaData = reader.ReadBytes(dnaLength);
                return cow;
            }
        }
    }

    // Composite key for breed and age indexing
    public class BreedAgeKey : IComparable<BreedAgeKey>
    {
        public string Breed { get; set; }
        public int Age { get; set; }

        public int CompareTo(BreedAgeKey other)
        {
            if (other == null) return 1;
            var breedComparison = string.Compare(Breed, other.Breed, StringComparison.Ordinal);
            return breedComparison != 0 ? breedComparison : Age.CompareTo(other.Age);
        }

        public override bool Equals(object obj)
        {
            return obj is BreedAgeKey other && CompareTo(other) == 0;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Breed, Age);
        }
    }

    // Main Database Implementation
    public class CowDatabase : ICowDatabase, IDisposable
    {
        private readonly IRecordStorage _recordStorage;
        private readonly IIndex<Guid, uint> _primaryIndex;
        private readonly IIndex<BreedAgeKey, uint> _secondaryIndex;

        public CowDatabase(string databasePath)
        {
            var stream = new FileStream(databasePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            var blockStorage = new BlockStorage(stream);
            _recordStorage = new RecordStorage(blockStorage);
            _primaryIndex = new MemoryIndex<Guid, uint>();
            _secondaryIndex = new MemoryIndex<BreedAgeKey, uint>();
        }

        public void Insert(CowModel cow)
        {
            var data = CowSerializer.Serialize(cow);
            var recordId = _recordStorage.Create(data);
            
            _primaryIndex.Insert(cow.Id, recordId);
            _secondaryIndex.Insert(new BreedAgeKey { Breed = cow.Breed, Age = cow.Age }, recordId);
        }

        public void Delete(CowModel cow)
        {
            var entry = _primaryIndex.Get(cow.Id);
            if (entry != null)
            {
                _recordStorage.Delete(entry.Item2);
                _primaryIndex.Delete(cow.Id);
                _secondaryIndex.Delete(new BreedAgeKey { Breed = cow.Breed, Age = cow.Age }, entry.Item2);
            }
        }

        public void Update(CowModel cow)
        {
            var entry = _primaryIndex.Get(cow.Id);
            if (entry != null)
            {
                var data = CowSerializer.Serialize(cow);
                _recordStorage.Update(entry.Item2, data);
                // Note: If breed or age changed, we'd need to update secondary index
            }
        }

        public CowModel Find(Guid id)
        {
            var entry = _primaryIndex.Get(id);
            if (entry != null)
            {
                var data = _recordStorage.Find(entry.Item2);
                return data != null ? CowSerializer.Deserialize(data) : null;
            }
            return null;
        }

        public IEnumerable<CowModel> FindBy(string breed, int age)
        {
            var key = new BreedAgeKey { Breed = breed, Age = age };
            var entries = _secondaryIndex.LargerThanOrEqualTo(key)
                                        .TakeWhile(e => e.Item1.Breed == breed && e.Item1.Age == age);
            
            foreach (var entry in entries)
            {
                var data = _recordStorage.Find(entry.Item2);
                if (data != null)
                    yield return CowSerializer.Deserialize(data);
            }
        }

        public void Dispose()
        {
            // In a real implementation, we'd properly dispose of file streams and flush indexes
        }
    }

    // Example usage
    class Program
    {
        static void Main(string[] args)
        {
            using (var db = new CowDatabase("cows.db"))
            {
                // Insert some cows
                var cow1 = new CowModel
                {
                    Id = Guid.NewGuid(),
                    Breed = "Holstein",
                    Age = 3,
                    Name = "Bessie",
                    DnaData = Encoding.UTF8.GetBytes("ATCGATCGATCG")
                };

                var cow2 = new CowModel
                {
                    Id = Guid.NewGuid(),
                    Breed = "Angus",
                    Age = 5,
                    Name = "Ferdinand",
                    DnaData = Encoding.UTF8.GetBytes("GCTAGCTAGCTA")
                };

                db.Insert(cow1);
                db.Insert(cow2);

                // Find by ID
                var foundCow = db.Find(cow1.Id);
                Console.WriteLine($"Found cow: {foundCow?.Name}");

                // Find by breed and age
                var holsteins = db.FindBy("Holstein", 3);
                foreach (var cow in holsteins)
                {
                    Console.WriteLine($"Holstein cow: {cow.Name}");
                }
            }
        }
    }
}