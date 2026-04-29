select phylum, class, ord, family, genus
from Taxonomy natural join Observation
where sid = '103-buccal-10_27_20'