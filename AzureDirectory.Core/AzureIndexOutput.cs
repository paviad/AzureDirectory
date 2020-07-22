using System;
using System.Linq;
using Force.Crc32;
using Lucene.Net.Store;
using Lucene.Net.Support;
using Microsoft.Azure.Storage.Blob;

namespace AzureDirectory.Core {
    /// <summary>
    /// Implements IndexOutput semantics for a write/append only file
    /// </summary>
    public class AzureIndexOutput : IndexOutput {
        private readonly CloudBlobStream _stream;
        private uint _crc;

        public AzureIndexOutput(CloudBlockBlob blob) {
            _stream = blob.OpenWrite();
        }

        public override void WriteByte(byte b) {
            _stream.WriteByte(b);
            _crc = Crc32CAlgorithm.Append(_crc, new[] { b });
        }

        public override void WriteBytes(byte[] b, int offset, int length) {
            _stream.Write(b, offset, length);
            _crc = Crc32CAlgorithm.Append(_crc, b, offset, length);
        }

        public override void Flush() {
            try {
                _stream.Flush();
            }
            catch {
                // ignored
            }
        }

        protected override void Dispose(bool disposing) {
            _stream?.Dispose();
        }

        public override long GetFilePointer() {
            return _stream.Position;
        }

        [Obsolete]
        public override void Seek(long pos) {
            throw new NotSupportedException();
        }

        public override long Checksum => _crc;
    }
}
