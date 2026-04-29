select phylum as source, class as target, count(*) as value
from Taxonomy natural join Observation
where sid = %s
and abundance_counts > 0
and phylum is not NULL 
and class is not NULL
group by phylum, class 

union all

select class as source, ord as target, count(*) as value
from Taxonomy natural join Observation
where sid = %s
and abundance_counts > 0
and class is not null 
and ord is not null
group by class, ord 

union all

select ord as source, family as target, count(*) as value
from Taxonomy natural join Observation
where sid = %s
and abundance_counts > 0
and ord is not null 
and family is not null
group by ord, family

union all

select family as source, genus as target, count(*) as value
from Taxonomy natural join Observation
where sid = %s
and abundance_counts > 0
and family is not null 
and genus is not null
group by family, genus

order by source, target;