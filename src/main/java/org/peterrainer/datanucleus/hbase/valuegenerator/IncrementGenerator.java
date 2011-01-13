package org.peterrainer.datanucleus.hbase.valuegenerator;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ObjectManagerFactoryImpl;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.xml.MetaDataParser;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

public class IncrementGenerator  implements ValueGenerator {
    /** Symbolic name for the sequence. */
    private String name;

    /** Properties controlling the generator behaviour. */
    private Properties properties;
    
    /** Key used in the Table to access the increment count */
    private String key;
    
    private HBaseAdmin admin;
    
    private HTable table;
    
    private long current = -1;
    
    private PersistenceConfiguration conf = null;
    
    private String tableName = null;
    
    /**
     * Constructor.
     * Will receive the following properties (as a minimum) through this constructor.
     * <ul>
     * <li>class-name : Name of the class whose object is being inserted.</li>
     * <li>root-class-name : Name of the root class in this inheritance tree</li>
     * <li>field-name : Name of the field with the strategy (unless datastore identity field)</li>
     * <li>catalog-name : Catalog of the table (if specified)</li>
     * <li>schema-name : Schema of the table (if specified)</li>
     * <li>table-name : Name of the root table for this inheritance tree (containing the field).</li>
     * <li>column-name : Name of the column in the table (for the field)</li>
     * <li>sequence-name : Name of the sequence (if specified in MetaData as "sequence)</li>
     * </ul>
     * 
     * @param name Symbolic name for this generator
     * @param props Properties controlling the behaviour of the generator (or null if not required).
     */
    public IncrementGenerator(String name, Properties props)
    {
        this.name = name;
        this.properties = props;
        this.key = props.getProperty("field-name", name);
    	this.tableName = "IncrementTable";
    }
	
	public String getName() {
		return this.name;
	}

	public Object next() {
		return nextValue();
	}

	public void allocate(int additional) {
		if(this.table == null) {
			this.init();
		}
	}

	public Object current() {
		if(this.table == null) {
			this.init();
		}
		return this.current();
	}

	public long nextValue() {
		if(this.table == null) {
			this.init();
		}
		
		try {
			this.current = table.incrementColumnValue(Bytes.toBytes(key), Bytes.toBytes("increment"), Bytes.toBytes("increment"), 1);
		} catch (IOException ex) {
			NucleusLogger.DATASTORE_PERSIST.error("IncrementGenerator: Error incrementing generated value", ex);
			throw new NucleusDataStoreException("Error incrementing generated value.", ex);
		}
		
		return this.current;
	}

	public long currentValue() {
		if(this.table == null) {
			this.init();
		}
		
		if(this.current < 0 ) {
			throw new NucleusDataStoreException();
		}
		
		return this.current;
	}

	private synchronized void init() {
		if(this.table == null) {
			try {				
				Configuration config = HBaseConfiguration.create();
				this.admin = new HBaseAdmin( config );
				
				NucleusLogger.DATASTORE_PERSIST.info("IncrementGenerator: Check if Table '"+this.tableName+"' exists");
				if(!admin.tableExists(this.tableName)) {
					NucleusLogger.DATASTORE_PERSIST.info("IncrementGenerator: Create Table '"+this.tableName+"'");
					HTableDescriptor ht = new HTableDescriptor( this.tableName );
					HColumnDescriptor hcd = new HColumnDescriptor( "increment" );
					hcd.setCompressionType(Algorithm.NONE);
					hcd.setMaxVersions(1);
					ht.addFamily( hcd );		
					admin.createTable(ht);		
					NucleusLogger.DATASTORE_PERSIST.info("IncrementGenerator: Table '"+this.tableName+"' created");
				}
			
				NucleusLogger.DATASTORE_PERSIST.info("IncrementGenerator: Getting Table");
				this.table = new HTable(config, this.tableName);
				NucleusLogger.DATASTORE_PERSIST.info("IncrementGenerator: Got Table");
				if(!this.table.exists(new Get(Bytes.toBytes(key)))) {
					NucleusLogger.DATASTORE_PERSIST.info("IncrementGenerator: Putting Start record into table '"+this.tableName+"' with key '"+key+"'");
					this.table.put(new Put(Bytes.toBytes(key)).add(Bytes.toBytes("increment"), Bytes.toBytes("increment"), Bytes.toBytes(0l)));
					NucleusLogger.DATASTORE_PERSIST.info("IncrementGenerator: Put Start into table '"+this.tableName+"' record with key '"+key+"'");
				}
			}
			catch(IOException ex) {
				NucleusLogger.CONNECTION.fatal("Error instantiating IncrementGenerator", ex);
			}
		}
	}
}