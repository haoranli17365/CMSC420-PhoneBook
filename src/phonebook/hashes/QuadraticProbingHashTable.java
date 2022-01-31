package phonebook.hashes;

import java.util.ArrayList;


import phonebook.utils.KVPair;
import phonebook.utils.PrimeGenerator;
import phonebook.utils.Probes;

/**
 * <p>{@link QuadraticProbingHashTable} is an Openly Addressed {@link HashTable} which uses <b>Quadratic
 * Probing</b> as its collision resolution strategy. Quadratic Probing differs from <b>Linear</b> Probing
 * in that collisions are resolved by taking &quot; jumps &quot; on the hash table, the length of which
 * determined by an increasing polynomial factor. For example, during a key insertion which generates
 * several collisions, the first collision will be resolved by moving 1^2 + 1 = 2 positions over from
 * the originally hashed address (like Linear Probing), the second one will be resolved by moving
 * 2^2 + 2= 6 positions over from our hashed address, the third one by moving 3^2 + 3 = 12 positions over, etc.
 * </p>
 *
 * <p>By using this collision resolution technique, {@link QuadraticProbingHashTable} aims to get rid of the
 * &quot;key clustering &quot; problem that {@link LinearProbingHashTable} suffers from. Leaving more
 * space in between memory probes allows other keys to be inserted without many collisions. The tradeoff
 * is that, in doing so, {@link QuadraticProbingHashTable} sacrifices <em>cache locality</em>.</p>
 *
 * @author Haoran Li
 *
 * @see HashTable
 * @see SeparateChainingHashTable
 * @see OrderedLinearProbingHashTable
 * @see LinearProbingHashTable
 * @see CollisionResolver
 */
public class QuadraticProbingHashTable extends OpenAddressingHashTable {

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
     * @param soft A boolean indicator of whether we want to use soft deletion or not. {@code true} if and only if
     *               we want soft deletion, {@code false} otherwise.
     */
    public QuadraticProbingHashTable(boolean soft) {
        this.is_soft = soft;
        this.count = 0;
        this.tombstone_counter = 0;
        this.primeGenerator = new PrimeGenerator();
        this.table = new KVPair[primeGenerator.getCurrPrime()];
    }

    @Override
    public Probes put(String key, String value) {
        if (key == null || value == null){
            throw new IllegalArgumentException();
        }else{
            
            // if resize is needed
            int probes_counter = 0;
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
                // re-insert to new table
                if (temp.size() != 0){
                    for(KVPair p : temp){
                        probes_counter += put(p.getKey(),p.getValue()).getProbes();
                    }
                }
            }
            // insert element
            int target_address = this.hash(key);
            System.out.println(key + " : " + this.hash(key));
            KVPair new_pair = new KVPair(key, value);
            int acc_idx = 2;
            // find the next null space for inserting the target element
            while(this.table[target_address] != null){
                probes_counter ++;
                target_address = (this.hash(key) + (acc_idx - 1) + ((acc_idx - 1)*(acc_idx - 1))) % this.table.length;
                acc_idx ++;
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
        int start_idx = this.hash(key); // start index marker
        int probes_counter = 1;
        int acc_idx = 2;
        while(this.table[target_address] != null){
            if (acc_idx != 2 && target_address == start_idx){ // search fail, wrap around to the start index
                return new Probes(null, probes_counter);
            }
            if (this.table[target_address].getKey().equals(key) && this.table[target_address] != this.TOMBSTONE){
                return new Probes(table[target_address].getValue(), probes_counter);
            }
            probes_counter ++;
            target_address = (this.hash(key) + (acc_idx - 1) + ((acc_idx - 1)*(acc_idx - 1))) % this.table.length;
            acc_idx ++;
        }
        return new Probes(null, probes_counter);
    }

    @Override
    public Probes remove(String key) {
        int probes_counter = 1;
        int target_address = this.hash(key);
        int acc_idx = 2;
        
        if (this.is_soft){ // soft deletion
            int start_idx = this.hash(key); // start index marker.
            
            while(this.table[target_address] != null){
                // after going over the whole table but not find the target, search fail.
                if (acc_idx != 2 && target_address == start_idx){
                    return new Probes(null, probes_counter); 
                }
                // if current key is the one to be deleted, and it's not previously set as a TOMBSTONE.
                if(this.table[target_address].getKey().equals(key) && this.table[target_address] != this.TOMBSTONE){
                    String ret_value = this.table[target_address].getValue();
                    this.table[target_address] = this.TOMBSTONE; // set the target to TOMBSTONE
                    this.tombstone_counter ++;
                    return new Probes(ret_value, probes_counter);
                }
                probes_counter ++;
                target_address = (this.hash(key) + (acc_idx - 1) + ((acc_idx - 1)*(acc_idx - 1))) % this.table.length;
                acc_idx ++;
            }
            return new Probes(null, probes_counter);
        }else{ // hard deletion
            /**
             * 1. set target to null.
             * 2. loop through the table find remining elements, take them out and empty the table.
             * 3. re-insert element back into the table.
             */
            while(this.table[target_address] != null){
                // if current space is the target, set it to null
                if (this.table[target_address].getKey().equals(key)){
                    String ret_val = this.table[target_address].getValue();
                    this.table[target_address] = null; // set target to null.
                    // re-insertion
                    this.count = 0;
                    ArrayList<KVPair> temp = new ArrayList<>();
                    for(KVPair pair : this.table){ // take out every element in table.
                        if(pair != null){
                            temp.add(pair);
                        }
                        probes_counter++; // probes for searching remaining elements.
                    }
                    this.table = new KVPair[this.primeGenerator.getCurrPrime()]; // create new table for re-insertion
                    if (temp.size() != 0){
                        for(KVPair pair : temp){ // re-inserting every element.
                            probes_counter += (this.put(pair.getKey(),pair.getValue())).getProbes(); 
                        }
                    }
                    return new Probes(ret_val, probes_counter);
                }
                probes_counter ++;
                target_address = (this.hash(key) + (acc_idx - 1) + ((acc_idx - 1)*(acc_idx - 1))) % this.table.length;
                acc_idx ++;
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
    public int size(){
        return this.count - this.tombstone_counter;
    }

    @Override
    public int capacity() {
        return this.table.length;
    }

}