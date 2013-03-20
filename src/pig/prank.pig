-- a script that implements the page rank algorthm

--%default INPUT 'wat/WEB-20130124130848014-00000-31940~s3scape01~8083.wat.gz';
%default INPUT 'wat';
%default OUTPUT 'pagerank';
%default OUTPUT2 'pagerank_summary';

SET pig.splitCombination 'false';

-- REGISTER archive-meta-extractor-20110512.jar;
REGISTER archive-meta-extractor-20110609.jar;

-- alias short-hand for IA 'resolve()' UDF:
DEFINE resolve org.archive.hadoop.func.URLResolverFunc();

-- load data from INPUT:
orig = LOAD '$INPUT' USING org.archive.hadoop.ArchiveJSONViewLoader('Envelope.WARC-Header-Metadata.WARC-Target-URI','Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Head.Base','Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.@Links.{url,path,text,alt}') AS (src:chararray,html_base:chararray,relative:chararray,path:chararray,text:chararray,alt:chararray);

-- discard sources which have no links
links_only = FILTER orig BY relative != '';

-- converts relative URL to absolute URL
resolved_links = FOREACH links_only GENERATE src, resolve(src,html_base,relative) AS dst;

------------------------------------------------------------------------
-- START: uncomment to calculate on domain level (and not on page-level)
------------------------------------------------------------------------
-- TODO: write a UDF for this task
-- TODO add regex for IP addresses to the query string!

-- extract domain names
resolved_links = FOREACH resolved_links { 
	a = REGEX_EXTRACT(src,'(^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6}))(.*)', 1);
	b = REGEX_EXTRACT(dst,'(^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6}))(.*)', 1);
	GENERATE a as src, b as dst;
};

-- remove entries which did not pass the domainlevel extraction correctly
resolved_links = FILTER resolved_links BY (src != '' AND dst != '');

------------------------------------------------------------------------
-- END: uncomment to calculate on domain level (and not on page-level)
------------------------------------------------------------------------

-- Cononicalize urls, trim, trailing slashes, hashes, etc. (without discarding these urls)
resolved_links = FOREACH resolved_links { 
	a = REGEX_EXTRACT(src,'(.*)(#.*$)', 1); 
	b = REGEX_EXTRACT(dst,'(.*)(#.*$)', 1); 
	GENERATE ((a is null) ? src : a ) as src, ((b is null) ? dst : b ) as dst; 
};

-- ResolvedLinks = FOREACH ResolvedLinks { 
src = FOREACH resolved_links { 
	a = REGEX_EXTRACT(src,'(.*)([#\\/\\?]$)', 1); 
	b = REGEX_EXTRACT(dst,'(.*)([#\\/\\?]$)', 1);
	GENERATE ((a is null) ? src : a) as src, ((b is null) ? dst : b) as dst; 
};

--------------------------------
-- START: Exclude dangling pages 
--------------------------------

-- Remove destinations urls which are not contained in list of sources urls
-- Remove this part for page level ranking 
-- TODO: Add them back after after page ranks have been calculated 
dsts = FOREACH src GENERATE dst;
dsts = DISTINCT dsts;  
dangling_pages = JOIN dsts BY dst LEFT OUTER, src BY src;
dangling_pages = filter dangling_pages by ($1 is null);
src = join src by dst left outer, dangling_pages by $0;
src = filter src by ($2 is null);
src = foreach src generate $0 as src, $1 as dst;

--------------------------------
-- end: Exclude dangling pages 
--------------------------------

-- Calculate number of resources 
xsrc = foreach src generate $0;
xsrc = distinct xsrc;

-- calculate number of nodes N
N = foreach (group xsrc all) generate COUNT(xsrc) as val;

-- optionally! calculate number of relations
R = foreach (group src all) generate COUNT(src) as val;

-- A workaround to create a scalar for d
d = foreach N generate 0.75 as val;


-- create initial page rank relateion
initial_pagerank = foreach (group src by $0) generate 1.0/N.val as pr, group as src, src.$1 as dst;

-- calculate weight for each link
outbound_pagerank = foreach initial_pagerank generate src, pr/COUNT($2) as pagerank, flatten($2) as to_url;

-- group weighted links by destination
intermediate = cogroup outbound_pagerank by to_url, initial_pagerank by src inner;

-- calculate new page rank
new_pagerank = foreach intermediate {
 inlinks = COUNT(outbound_pagerank.pagerank);
 generate group as url, ( (inlinks > 0) ? (1-d.val)/N.val + d.val * SUM(outbound_pagerank.pagerank) : (1-d.val)/N.val ) as pagerank, flatten(initial_pagerank.dst) as links;
};

----------------------------
-- start: another iteration
----------------------------
outbound_pagerank = foreach new_pagerank generate url, pagerank/COUNT($2) as pagerank, flatten($2) as to_url;
new_pagerank = foreach (cogroup outbound_pagerank by to_url, new_pagerank by url inner) {
 inlinks = COUNT(outbound_pagerank.pagerank);
 generate group as url, ((inlinks > 0) ? (1-0.75)/N.val + 0.75 * SUM(outbound_pagerank.pagerank) : (1-0.75)/N.val ) as pagerank, flatten(new_pagerank.links) as links; 
};

----------------------------
-- end: another iteration
----------------------------

-- no flattening before writing results to file
outbound_pagerank = foreach new_pagerank generate url, pagerank/COUNT($2) as pagerank, flatten($2) as to_url;
new_pagerank = foreach (cogroup outbound_pagerank by to_url, new_pagerank by url inner) {
 inlinks = COUNT(outbound_pagerank.pagerank);
 generate group as url, ((inlinks > 0) ? (1-0.75)/N.val + 0.75 * SUM(outbound_pagerank.pagerank) : (1-0.75)/N.val ) as pagerank; 
};


-- order pagerank
new_pagerank = ORDER new_pagerank BY pagerank DESC;

-- aggregate some results
S = foreach (group new_pagerank all) generate 'S(PR):', SUM(new_pagerank.pagerank) as S, 'N:', N.$0 as N, 'S(L):', R.val as R;

-- write results
STORE new_pagerank INTO '$OUTPUT';
STORE S INTO '$OUTPUT2';
