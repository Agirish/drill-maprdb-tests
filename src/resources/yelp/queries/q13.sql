select b.name from business b where b.business_id in (SELECT r.business_id FROM review r
GROUP BY r.business_id having sum(r.votes.cool) > 2000 order by sum(r.votes.cool)  desc);
