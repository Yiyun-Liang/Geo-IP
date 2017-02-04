register file:/usr/local/Cellar/pig/0.16.0/libexec/lib/piggybank.jar;
register file:/usr/local/Cellar/pig/0.16.0/libexec/lib/elasticsearch-hadoop-pig-2.1.0.jar;

%define ESSTORAGE org.elasticsearch.hadoop.pig.EsStorage()

FORUM_DATA = LOAD '../big-data/access_log' USING TextLoader as (line:chararray);

LOG = FOREACH FORUM_DATA GENERATE
  FLATTEN(
    REGEX_EXTRACT_ALL(line, '(\\S+) - - \\[([^\\[]+)\\]\\s+"([^"]+)"\\s+(\\d+)\\s+(\\d+)\\s+')
  )
  AS (
      ip: chararray,
      timestamp: chararray,
      url: chararray,
      status: chararray,
      bytes: chararray
  );

A = FOREACH LOG GENERATE ToDate(timestamp, 'dd/MMM/yyyy:HH:mm:ss Z')
  as date, ip, url, (int)status, (int)bytes;
--B = GROUP A BY (timestamp);
--C = FOREACH B GENERATE FLATTEN(group) as (timestamp), COUNT(A) as count;
--D = ORDER C BY timestamp, count desc;
STORE A INTO 'cssa/log' USING $ESSTORAGE;
