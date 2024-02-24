using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace SBD_Proj2
{
    [Serializable]
    internal class Buffer
    {
        protected byte[] _content;
        public byte[] Content { get => _content; }


        public Buffer()
        {
            _content = Array.Empty<byte>();
        }


        public virtual void Clear()
        {
            for (int i = 0; i < Content.Length; i++)
            {
                Content[i] = 0;
            }
        }

        public Buffer DeepCopy()
        {
            BinaryFormatter formatter = new();
            using MemoryStream stream = new();

            formatter.Serialize(stream, this);
            stream.Seek(0, SeekOrigin.Begin);
            return (Buffer)formatter.Deserialize(stream);
        }
    }
}
