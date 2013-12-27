
/**
 * kvlayer only uses the python class, which is compiled via the
 * kvlayer/setup.py, and we include cpp and java so that one could use
 * this thrift file without modification when implementing serializers
 * in other languages.
 */
namespace py kvlayer.instance_collection.blob_collection
namespace cpp kvlayer.instance_collection.blob_collection
namespace java kvlayer.instance_collection.blob_collection

struct TypedBlob {
  /**
   * name of the serializer for reading/writing this blob.
   * Serializers can be implemented in multiple programming languages,
   * e.g. yaml and json.
   */
  1: string serializer,

  /**
   * bytes that serializer can load to make an in-memory object
   */
  2: binary blob,

  /**
   * dictionary of configuration information to pass into the
   * serializer.  For example, in python, this is passed to
   * my_serializer.configure(config)
   */
  3: optional map<string, string> config = {},

}

/**
 * BlobCollection is a simple thrift struct containing a map that
 * carries TypedBlobs of data that can be lazily deserialized as
 * needed.  Thrift handles the fast (de)marshalling of long strings.
 * By making this a thrift class, we can also utilize the
 * streamcorpus.Chunk convenience methods.
 */
struct BlobCollection {
  1: map<string, TypedBlob> typed_blobs = {},

}

// While we could make this a typedef, I don't that would work with
// streamcorpus.Chunk and doesn't leave room for adding more fields.
