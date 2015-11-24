select name, state,city, `review_count` from business where review_count > 1000 order by `review_count` desc limit 10;
