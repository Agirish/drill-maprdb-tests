select `name`, `type`, flatten(kvgen(`compliments`)) as `compliments` from `user` order by `name`, `type` limit 20;
