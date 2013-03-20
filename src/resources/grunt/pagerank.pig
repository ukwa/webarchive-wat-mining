-- a set of pig statements to experiment with page rank using the grunt shell

-- load test data from file
src = load 'pages' as (src:chararray, dst:chararray);

src = filter src by dst != '';

-- remove dangling links
dsts = foreach src generate dst;
dsts = distinct dsts;  
dangling_pages = join dsts by dst left outer, src by src;
dangling_pages = filter dangling_pages by ($1 is null);
src = join src by dst left outer, dangling_pages by $0;
src = filter src by ($2 is null);
src = foreach src generate $0 as src, $1 as dst;

-- calculate N
xsrc = foreach src generate $0;
xsrc = distinct xsrc;
N = foreach (group xsrc all) generate COUNT(xsrc) as val;

-- calculate and print inital rank
initial_pagerank = foreach (group src by $0) generate 1.0/N.val as pr, group as src, src.$1 as dst;

dump initial_pagerank;

-- calculate first iteration
outbound_pagerank = foreach initial_pagerank generate src, pr/COUNT($2) as pagerank, flatten($2) as to_url;

intermediate = cogroup outbound_pagerank by to_url, initial_pagerank by src inner;

new_pagerank = foreach intermediate {
 inlinks = COUNT(outbound_pagerank.pagerank);
 generate group as url, ((inlinks > 0) ? (1-0.75)/N.val + 0.75 * SUM(outbound_pagerank.pagerank) : (1-0.75)/N.val ) as pagerank, flatten(initial_pagerank.dst) as links;
};

-- calculate another iteration
outbound_pagerank = foreach new_pagerank generate url, pagerank/COUNT($2) as pagerank, flatten($2) as to_url;

new_pagerank = foreach (cogroup outbound_pagerank by to_url, new_pagerank by url inner) {
 inlinks = COUNT(outbound_pagerank.pagerank);
 generate group as url, ((inlinks > 0) ? (1-0.75)/N.val + 0.75 * SUM(outbound_pagerank.pagerank) : (1-0.75)/N.val ) as pagerank, flatten(new_pagerank.links) as links; 
};

-- print result
dump new_pagerank;
