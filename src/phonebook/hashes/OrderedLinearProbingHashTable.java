package phonebook.hashes;

import java.util.ArrayList;

import phonebook.exceptions.UnimplementedMethodException;
import phonebook.utils.KVPair;
import phonebook.utils.PrimeGenerator;
import phonebook.utils.Probes;

/**
 * <p>{@link OrderedLinearProbingHashTable} is an Openly Addressed {@link HashTable} implemented with
 * <b>Ordered Linear Probing</b> as its collision resolution strategy: every key collision is resolved by moving
 * one address over, and the keys in the chain is in order. It suffer from the &quot; clustering &quot; problem:
 * collision resolutions tend to cluster collision chains locally, making it hard for new keys to be
 * inserted without collisions. {@link QuadraticProbingHashTable} is a {@link HashTable} that
 * tries to avoid this problem, albeit sacrificing cache locality.</p>
 *
 * @author Haoran Li
 *
 * @see HashTable
 * @see SeparateChainingHashTable
 * @see LinearProbingHashTable
 * @see QuadraticProbingHashTable
 * @see CollisionResolver
 */
public class OrderedLinearProbingHashTable extends OpenAddressingHashTable {

    /* ********************************************************************/
    /* ** INSERT ANY PRIVATE METHODS OR FIELDS YOU WANT TO USE HERE: ******/
    /* ********************************************************************/
    private boolean is_soft;
    private int tombstone_counter;
    /* ******************************************/
    /*  IMPLEMENT THE FOLLOWING PUBLIC METHODS: */
    /* **************************************** */

    /**
     * Constructor with soft deletion option. Initializes the internal storage with a size equal to the starting value of  {@link PrimeGenerator}.
     * @param soft A boolean indicator of whether we want to use soft deletion or not. {@code true} if and only if
     *               we want soft deletion, {@code false} otherwise.
     */
    public OrderedLinearProbingHashTable(boolean soft){
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
     *
     * Different from {@link LinearProbingHashTable}, the keys in the chain are <b>in order</b>. As a result, we might increase
     * the cost of insertion and reduce the cost on search miss. One thing to notice is that, in soft deletion, we ignore
     * the tombstone during the reordering of the keys in the chain. We will have some example in the writeup.
     *
     * Instances of {@link OrderedLinearProbingHashTable} will follow the writeup's guidelines about how to internally resize
     * the hash table when the capacity exceeds 50&#37;
     * @param key The record's key.
     * @param value The record's value.
     * @throws IllegalArgumentException if either argument is {@code null}.
     * @return The {@link phonebook.utils.Probes} with the value added and the number of probes it makes.
     */
    @Override
    public Probes put(String key, String value) {
        if (key == null || value == null){
            throw new IllegalArgumentException();
        }else{
            int probes_counter = 0;
            // if resize is needed
            if (0.5 < ((double)this.count)/(double)this.table.length){
                ArrayList<KVPair> temp = new ArrayList<>();
                for (KVPair pair : this.table){
                    if(pair != null && pair != this.TOMBSTONE){
                        temp.add(pair);
                    }
                    probes_counter ++;
                }
                this.count = 0;
                this.tombstone_counter = 0;
                this.table = new KVPair[this.primeGenerator.getNextPrime()];
                if (temp.size() != 0){
                    for(KVPair p : temp){
                        probes_counter += put(p.getKey(),p.getValue()).getProbes();
                    }
                } 
            }
            // inserting the new element
            int target_address = this.hash(key);
            KVPair new_pair = new KVPair(key, value);
            while(this.table[target_address] != null){
                if (this.table[target_address].getKey().compareTo(new_pair.getKey()) > 0 && this.table[target_address] != this.TOMBSTONE){
                    KVPair temp_pair = this.table[target_address];
                    this.table[target_address] = new_pair;
                    new_pair = temp_pair;
                }else{
                    probes_counter ++;
                    target_address = (target_address + 1 == this.table.length) ? 0 : target_address + 1;
                }
            }
            probes_counter ++;
            this.table[target_address] = new_pair;
            this.count ++;
            return new Probes(value, probes_counter);
        }
    }

    @Override
    public Probes get(String key) {
        int target_address = this.hash(key);
        int probes_counter = 1;
        while(this.table[target_address] != null){
            if (this.table[target_address].getKey().compareTo(key) >= 0 && this.table[target_address] != this.TOMBSTONE){
                if (this.table[target_address].getKey().equals(key)){
                    return new Probes(table[target_address].getValue(), probes_counter);
                }else{
                    return new Probes(null, probes_counter);
                }
            }else{
                probes_counter ++;
                target_address = (target_address + 1 == this.table.length) ? 0 : target_address + 1;
            }
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
        int probes_counter = 1;
        int target_address = this.hash(key);
        if (this.is_soft){ // soft deletion
            while(this.table[target_address] != null){
                // if current key is the one to be deleted, and it's not previously set as a TOMBSTONE.
                if(this.table[target_address].getKey().compareTo(key) >= 0 && this.table[target_address] != this.TOMBSTONE){
                    if (this.table[target_address].getKey().equals(key)){
                        String ret_value = this.table[target_address].getValue();
                        this.table[target_address] = this.TOMBSTONE; // set the target to TOMBSTONE
                        this.tombstone_counter ++;
                        return new Probes(ret_value, probes_counter);
                    }else{
                        return new Probes(null, probes_counter); // fail to delete.
                    }
                }else{
                    probes_counter ++;
                    target_address = (target_address+1) % this.table.length;
                }
            }
            return new Probes(null, probes_counter);
        }else{ // hard deletion
            while(this.table[target_address] != null){
                if(this.table[target_address].getKey().compareTo(key) >= 0 && this.table[target_address] != this.TOMBSTONE){
                    // if current bucket is the target, set the bucket to null.
                    if (this.table[target_address].getKey().equals(key)){
                        String ret_val = this.table[target_address].getValue();
                        this.table[target_address] = null; // set bucket to null
                        this.count --;
                        // Find all the buckets in the cluster, save them into the temp arraylist
                        ArrayList<KVPair> temp = new ArrayList<>();
                        int pair_ptr = (target_address + 1) % this.table.length; // check for the next bucket in cluster
                        if(this.table[pair_ptr] == null){
                            probes_counter++; // 1 probe if no collision chain
                        }
                        while (this.table[pair_ptr] != null){
                            temp.add(this.table[pair_ptr]);
                            this.table[pair_ptr] = null;
                            this.count --; // remove all the elements in the cluster
                            pair_ptr = (pair_ptr + 1) % this.table.length;
                            probes_counter ++; // probe for checking the next non-null space
                            if (this.table[pair_ptr] == null){ // if next one is null, the last check count as one probe
                                probes_counter ++; 
                            }
                        }
                        // re-insert every buckets from the cluster.
                        if (temp.size() != 0){
                            for(KVPair curr_pair : temp){ 
                                probes_counter += (this.put(curr_pair.getKey(), curr_pair.getValue()).getProbes());
                            }
                        }
                        return new Probes(ret_val, probes_counter);
                    }else{
                        return new Probes(null, probes_counter); // fail to delete.
                    }
                }else{
                    probes_counter ++;
                    target_address = (target_address+1) % this.table.length;
                }
            }
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
            if(this.table[i].getValue().equals(value) && this.table[i] != this.TOMBSTONE){
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
