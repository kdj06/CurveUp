# 04_hbase.hb  -  HBase shell steps (run line by line in `hbase shell` or %hbase)
# Needs: $HBASE_HOME/bin/start-hbase.sh and jps showing HMaster, HRegionServer, HQuorumPeer (Lab 6)

# 1. Table: META keeps 1 version; MSG keeps 3 so a corrected message keeps its history
create 'CONVERSATION', {NAME => 'META', VERSIONS => 1}, {NAME => 'MSG', VERSIONS => 3}
describe 'CONVERSATION'

# 2. Load (run in a Terminal, not the HBase shell) - a MapReduce job that ships with HBase:
#   $HBASE_HOME/bin/hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
#     -Dimporttsv.columns=HBASE_ROW_KEY,META:program_id,META:channel,MSG:role,MSG:sender_type,MSG:source,MSG:ts_utc,MSG:intent,MSG:language,MSG:confidence,MSG:content \
#     CONVERSATION /datalake/curated/hbase_tsv

# 3. Checks
count 'CONVERSATION'                       # expect 1870

# 4. The use case: open one whole conversation, in time order, with one prefix scan
scan 'CONVERSATION', {ROWPREFIXFILTER => 't_3369d6bcd9c2fc97|'}

# 5. One message by full key (take a key from the scan above)
# get 'CONVERSATION', 't_3369d6bcd9c2fc97|<millis>|<message_id>'

# 6. Only the text and sender of one conversation
scan 'CONVERSATION', {ROWPREFIXFILTER => 't_3369d6bcd9c2fc97|', COLUMNS => ['MSG:sender_type', 'MSG:content']}

# 7. A staff member corrects a reply: a new version is kept, the old one stays readable
# put 'CONVERSATION', '<row key>', 'MSG:content', 'corrected text'
# get 'CONVERSATION', '<row key>', {COLUMN => 'MSG:content', VERSIONS => 3}

# 8. Counter-example for the report: "all staff replies" has no key to use, so HBase reads every row
scan 'CONVERSATION', {COLUMNS => ['MSG:sender_type'], FILTER => "SingleColumnValueFilter('MSG','sender_type',=,'binary:staff')"}
