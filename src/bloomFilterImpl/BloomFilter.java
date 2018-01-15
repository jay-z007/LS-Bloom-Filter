package bloomFilterImpl;

import java.util.*;
import org.apache.hadoop.util.hash.MurmurHash;
import java.math.*;
import java.nio.*;
 
class BloomFilter
{    
    private byte[] set;
    private int NoOfHash, m, size;
    private FNV fnv;
    private MurmurHash murmurHash;
 
    public BloomFilter(int capacity, int k)
    {
        m = capacity;
        set = new byte[m];
        NoOfHash = k;
        size = 0;
        
        murmurHash = new MurmurHash();
        fnv = new FNV();
        
    }
    
    /* Function to clear bloom set */
    public void makeEmpty()
    {
        set = new byte[m];
        size = 0;
        murmurHash = new MurmurHash();
        fnv = new FNV();
    }
    
    /* Function to check is empty */
    public boolean isEmpty()
    {
        return size == 0;
    }
    
    /* Function to get size of objects added */
    public int getSize()
    {
        return size;
    }
 
    /* Function to add an object */
    public void add(Object obj)
    {
        int[] tmpset = getSetArray(obj);
        for (int i : tmpset)
            set[i] = 1;
        size++;
        System.out.println("\n"+Arrays.toString(set));
    }
    
    /* Function to check is an object is present */
    public boolean contains(Object obj) 
    {
        int[] tmpset = getSetArray(obj);
        for (int i : tmpset)
            if (set[i] != 1)
                return false;
        return true;
    }
    
    /* Function to get set array for an object */
    private int[] getSetArray(Object obj)
    {
        int[] tmpset = new int[NoOfHash];
        int j = obj.hashCode();
        byte[] bytes = ByteBuffer.allocate(4).putInt(j).array();
        
        int a = murmurHash.hash(bytes);
        BigInteger b = fnv.fnv1a_64(bytes);
        
        System.out.println("\n\n"+obj.toString());
        
        for (int i = 0; i < NoOfHash; i++)
        {    
        	tmpset[i] = Math.abs((a+ i*b.intValue()) % m);
        	System.out.print(tmpset[i]+" ");
        }	
        
        return tmpset;
    }    

   /* public static void main(String argv[])
    {
    	BloomFilter bf = new BloomFilter(100, 4);
    	bf.add("jay");
    	bf.add("faizaan");
    	bf.add("deepali");
    	bf.add("hello");
    	System.out.println(bf.contains("jay"));
    	System.out.println(bf.contains("jai"));    	
    } */
    
}