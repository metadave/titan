package com.thinkaurelius.titan.diskstorage.riak;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.query.indexes.BinIndex;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.LimitedSelector;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


    /*
    mvn package -Dmaven.test.skip=true

    conf = new BaseConfiguration();
    conf.setProperty("storage.backend","riak");
    conf.setProperty("storage.hostname","127.0.0.1");
    g = TitanFactory.open(conf);
    juno = g.addVertex(null);

    juno.setProperty("name", "juno");
    jupiter = g.addVertex(null);
    jupiter.setProperty("name", "jupiter");
    married = g.addEdge(null, juno, jupiter, "married");

    g2 = GraphOfTheGodsFactory.load(g)

     */

public class RiakKeyValueStore implements KeyValueStore {
    IRiakClient riak;
    String bucketName;
    Bucket bucket;

    public RiakKeyValueStore(IRiakClient riak, String bucket) {
        // NO POOL YET, JUST SEEING IF IT WORKS!
        this.riak = riak;
        this.bucketName = bucket;
        System.out.println("Bucket name = " + bucket);
        try {
            this.bucket = riak.createBucket(bucketName).execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransaction txh) throws StorageException {
        System.out.println("getKeys");
        try {
            final Iterable<String> keys = bucket.keys();
            final Iterator<String> it = keys.iterator();
            RecordIterator<ByteBuffer> ri = new RecordIterator<ByteBuffer>() {
                @Override
                public boolean hasNext() throws StorageException {
                    return it.hasNext();
                }

                @Override
                public ByteBuffer next() throws StorageException {
                    return ByteBuffer.wrap(it.next().getBytes());
                }

                @Override
                public void close() throws StorageException {

                }
            };
            return ri;
        } catch (RiakException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, StoreTransaction txh) throws StorageException {
        System.out.println("getSlice/1");
        return getSlice(keyStart, keyEnd, Integer.MAX_VALUE, txh);
    }

    @Override
    public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, int limit, StoreTransaction txh) throws StorageException {
        System.out.println("getSlice/2");
        return getSlice(keyStart, keyEnd, new LimitedSelector(limit), txh);
    }

    private String getIndexName() {
        return this.bucketName + "_bin";
    }

    @Override
    public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd,
                                        KeySelector selector, StoreTransaction txh) throws StorageException {
        System.out.println("getSlice/3 on " + this.bucketName);
        //System.out.println("  Foo: " + ByteBufferUtil.toBitString(keyStart, " "));
        //System.out.println("  Bar: " + ByteBufferUtil.toBitString(keyEnd, " "));

        List<KeyValueEntry> result;
        result = new ArrayList<KeyValueEntry>();

//        byte[] colStartBytes = keyStart.hasRemaining() ? ByteBufferUtil.getArray(keyStart) : null;
//        byte[] colEndBytes = keyEnd.hasRemaining() ? ByteBufferUtil.getArray(keyEnd) : null;

        try {
            List<String> keylist =
                    bucket.fetchIndex(BinIndex.named(getIndexName()))
                                    .from(wrap(keyStart))
                                    .to(wrap(keyEnd))
                                    .execute();
            System.out.println("KEYLIST SIZE = " + keylist.size());
            for(String k : keylist) {
                //System.out.println(">>>>>>" + k);
                IRiakObject obj = bucket.fetch(k).execute();
                boolean skip = false;

                skip = !selector.include(unwrap(k));

                if (!skip) {
                    result.add(new KeyValueEntry(unwrap(k), unwrap(obj.getValueAsString())));
                }

                if (selector.reachedLimit()) {
                    break;
                }
            }
        } catch (RiakException e) {
            e.printStackTrace();
        }

        return result;
    }




    public ByteBuffer unwrap(String s) {
        try {
            byte[] bytes = s.getBytes("UTF-8");
            ByteBuffer result =  ByteBuffer.wrap(bytes, 0, bytes.length);
            result.rewind();
            return result;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String wrap(ByteBuffer b) {
        try {
            return new String(b.array(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void insert(ByteBuffer key, ByteBuffer value, StoreTransaction txh) throws StorageException {
        RiakObjectBuilder builder =
                RiakObjectBuilder.newBuilder(this.bucketName, wrap(key))
                        .withContentType("application/octet-stream")
                        .withValue(wrap(value))
                        .addIndex(getIndexName(), wrap(key));
        IRiakObject obj = builder.build();
        try {
            bucket.store(obj).execute();
        } catch (RiakRetryFailedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(ByteBuffer key, StoreTransaction txh) throws StorageException {
        String k = wrap(key);
        System.out.println("delete:"  + k);
        try {
            bucket.delete(k).execute();
        } catch (RiakException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ByteBuffer get(ByteBuffer key, StoreTransaction txh) throws StorageException {
        String theKey = wrap(key);
        System.out.println("Fetching " + theKey);
        if(theKey == null || theKey.equals("")) {
            return null;
        }
        try {
            IRiakObject obj = bucket.fetch(theKey).execute();
            return unwrap(obj.getValueAsString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransaction txh) throws StorageException {
        System.out.println("containsKey");
        try {
            Object o = bucket.fetch(wrap(key)).execute();
            if(o != null) {
                return true;
            } else {
                return false;
            }
        } catch (RiakRetryFailedException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer expectedValue, StoreTransaction txh) throws StorageException {
        System.out.println("acquireLock");
    }

    @Override
    public ByteBuffer[] getLocalKeyPartition() throws StorageException {
        System.out.println("LOCALKEYPARTITION");
        return new ByteBuffer[0];
    }

    @Override
    public String getName() {
        System.out.println("BUCKETNAME");
        return bucketName;
    }

    @Override
    public void close() throws StorageException {
        System.err.println("close() Not implemented");
    }
}
