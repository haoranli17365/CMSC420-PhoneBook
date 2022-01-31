package phonebook.hashes;

import java.util.ArrayList;


import phonebook.utils.KVPair;
import phonebook.utils.PrimeGenerator;
import phonebook.utils.Probes;

/**
 * <p>{@link LinearProbingHashTable} is an Openly Addressed {@link HashTable} implemented with <b>Linear Probing</b> as its
 * collision resolution strategy: every key collision is resolved by moving one address over. It is
 * the most famous collision resolution strategy, praised for its simplicity, theoretical properties
 * and cache locality. It <b>does</b>, however, suffer from the &quot; clustering &quot; problem:
 * collision resolutions tend to cluster collision chains locally, making it hard for new keys to be
 * inserted without collisions. {@link QuadraticProbingHashTable} is a {@link HashTable} that
 * tries to avoid this problem, albeit sacrificing cache locality.</p>
 *
 * @author Haoran Li
 *
 * @see HashTable
 * @see SeparateChainingHashTable
 * @see OrderedLinearProbingHashTable
 * @see QuadraticProbingHashTable
 * @see CollisionResolver
 */
public class LinearProbingHashTable extends OpenAddressingHashTable {

    /* ********************************************************************/
    /* ** INSERT ANY PRIVATE METHODS OR FIELDS YOU WANT TO USE HERE: ******/
    /* ********************************************************************/
    private boolean is_soft;
    private int tombstone_counter;

    /**
     * Public method : For debugging use only
     */
    public void printTable(){
        System.out.println("***********START*************");
        for(KVPair p : this.table){
            if(p != null){
                System.out.println(p.getKey()+ " : "+ p.getValue());
            }else{
                System.out.println("null");
            }
        }
        System.out.println("************END*************");
    }
    /* ******************************************/
    /*  IMPLEMENT THE FOLLOWING PUBLIC METHODS: */
    /* **************************************** */

    /**
     * Constructor with soft deletion option. Initializes the internal storage with a size equal to the starting value of  {@link PrimeGenerator}.
     *
     * @param soft A boolean indicator of whether we want to use soft deletion or not. {@code true} if and only if
     *             we want soft deletion, {@code false} otherwise.
     */
    public LinearProbingHashTable(boolean soft) {
        this.is_soft = soft;
        this.count = 0;
        this.tombstone_counter = 0;
        this.primeGenerator = new PrimeGenerator();
        this.table = new KVPair[primeGenerator.getCurrPrime()];
    }

    /**
     * Inserts the pair &lt;key, value&gt; into this. The container should <b>not</b> allow for {@code null}
     * keys and values, and we <b>will</b> test if you are throwing a {@link IllegalArgumentException} from your code
     * if this method is given {@code null} arguments! It is important that we establish that no {@code null} entries
     * can exist in our database because the semantics of {@link #get(String)} and {@link #remove(String)} are that they
     * return {@code null} if, and only if, their key parameter is {@code null}. This method is expected to run in <em>amortized
     * constant time</em>.
     * <p>
     * Instances of {@link LinearProbingHashTable} will follow the writeup's guidelines about how to internally resize
     * the hash table when the capacity exceeds 50&#37;
     *
     * @param key   The record's key.
     * @param value The record's value.
     * @return The {@link phonebook.utils.Probes} with the value added and the number of probes it makes.
     * @throws IllegalArgumentException if either argument is {@code null}.
     */
    @Override
    public Probes put(String key, String value) {
        if (key == null || value == null){
            throw new IllegalArgumentException();
        }else{
            int probes_counter = 0;
            System.out.println(key + " : " + this.hash(key));
            // if resize needed.
            if(0.5 < ((double)this.count)/(double)this.table.length){
                ArrayList<KVPair> temp = new ArrayList<>();
                for (KVPair pair : this.table){
                    if(pair != null && pair != TOMBSTONE){
                        temp.add(pair);
                    }
                    probes_counter ++;
                }
                this.count = 0;
                this.tombstone_counter = 0; // reset tombestone to 0 since we resized.
                this.table = new KVPair[this.primeGenerator.getNextPrime()];
                // re-insert
                if (temp.size() != 0){
                    for(KVPair pair : temp){
                        
                        // int temp_address = this.hash(pair.getKey());
                        // System.out.println("re-inserting: "+ pair.getKey() + " : " + temp_address);
                        // while(this.table[temp_address] != null){
                        //     probes_counter++;
                        //     temp_address = (temp_address + 1 == this.table.length) ? 0 : temp_address + 1;;
                        // }
                        // probes_counter ++;
                        // this.table[temp_address] = pair;
                        // this.count ++;
                        probes_counter += (this.put(pair.getKey(), pair.getValue())).getProbes();
                    }
                }

            }
            int empty_idx = this.hash(key); // address of the target.
            // inserting the new KVPair to correct space by using linear probing.
            // if current space is occupied, finding the next available space.8
            while(this.table[empty_idx] != null){
                probes_counter ++;
                empty_idx = (empty_idx + 1 == this.table.length) ? 0 : empty_idx + 1;
            }
            probes_counter ++; // probe for access the target bucket.
            this.table[empty_idx] = new KVPair(key, value);
            this.count ++;
            return new Probes(value, probes_counter);
        }
    }

    @Override
    public Probes get(String key) {
        int target_address = this.hash(key);
        int probes_counter = 1;
        while(this.table[target_address] != null){
            if (this.table[target_address].getKey().equals(key) && this.table[target_address] != this.TOMBSTONE){
                return new Probes(table[target_address].getValue(), probes_counter);
            }
            probes_counter ++;
            target_address = (target_address + 1 == this.table.length) ? 0 : target_address + 1;
        }
        return new Probes(null, probes_counter);
    }


    /**
     * <b>Return</b> and <b>remove</b> the value associated with key in the {@link HashTable}. If key does not exist in the database
     * or if key = {@code null}, this method returns {@code null}. This method is expected to run in <em>amortized constant time</em>.
     *
     * @param key The key to search for.
     * @return The {@link phonebook.utils.Probes} with associated value and the number of probe used. If the key is {@code null}, return value {@code null}
     * and 0 as number of probes; if the key dones't exists in the database, return {@code null} and the number of probes used.
     */
    @Override
    public Probes remove(String key) {
        int target_address = this.hash(key);
        int probes_counter = 1;
        if (this.is_soft){ // soft deletion
            while(this.table[target_address] != null){
                // if current key is the one to be deleted, and it's not previously set as a TOMBSTONE.
                if(this.table[target_address].getKey().equals(key) && this.table[target_address] != this.TOMBSTONE){
                    Probes ret_probe = new Probes(this.table[target_address].getValue(), probes_counter);
                    this.table[target_address] = this.TOMBSTONE; // set the target to TOMBSTONE
                    this.tombstone_counter ++;
                    return ret_probe;
                }else{
                    probes_counter ++;
                    target_address = (target_address+1) % this.table.length;
                }
            }
            return new Probes(null, probes_counter);
        }else{ // hard deletion
            while(this.table[target_address] != null){ // first hash(key) check is count as a probe
                // if current space is the target, set it to null
                if (this.table[target_address].getKey().equals(key)){
                    String ret_val = this.table[target_address].getValue();
                    this.table[target_address] = null; // set target to null
                    // re-insertion
                    this.count --;
                    ArrayList<KVPair> temp = new ArrayList<>();
                    // check the next bucket in the cluster, save into the temp arraylist.
                    int pair_ptr = (target_address+1) % this.table.length; 
                    if(this.table[pair_ptr] == null){
                        probes_counter++; // 1 probe if no collision chain
                    }
                    while (this.table[pair_ptr] != null){
                        temp.add(this.table[pair_ptr]);
                        this.table[pair_ptr] = null;
                        this.count --; // remove all buckets in the cluster from the table 
                        pair_ptr = (pair_ptr + 1) % this.table.length; 
                        probes_counter ++; // probe for checking the next non-null space
                        if (this.table[pair_ptr] == null){ // if next one is null, the last check count as one probe
                            probes_counter ++; 
                        }
                    }
                    // re-insert every buckets in the cluster.
                    if (temp.size() != 0){
                        for(KVPair curr_pair : temp){ 
                            probes_counter += (this.put(curr_pair.getKey(), curr_pair.getValue()).getProbes());
                        }
                    }
                    return new Probes(ret_val, probes_counter);
                } 
                // if current is not the target, then search the next one.
                probes_counter ++;
                target_address = (target_address + 1) % this.table.length;
            }
            // if target is not found, return null probe.
            return new Probes(null, probes_counter);
        }
    }
    
    @Override
    public boolean containsKey(String key) {
        return (this.get(key).getValue() != null) ? true : false;
    }

    @Override
    public boolean containsValue(String value) {
        for(int i=0;i<this.table.length;i++){
            if (this.table[i].getValue().equals(value) && this.table[i] != this.TOMBSTONE){
                return true;
            }
        }
        return false;
    }

    @Override
    public int size() {
        return this.count - this.tombstone_counter;
    }

    @Override
    public int capacity() {
        return this.table.length;
    }
}
