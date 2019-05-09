with source as (
    select min(a) as min,
           max(a) as max
      from ttmatrices.path_cost
),
     histogram as (
   select width_bucket(a, min, max, 9) as bucket,
          int4range(min(a), max(a), '[]') as range,
          count(*) as freq
     from ttmatrices.path_cost, source
 group by bucket
 order by bucket
)
 select bucket, range, freq,
        repeat('■',
               (   freq::float
                 / max(freq) over()
                 * 30
               )::int
        ) as bar
   from histogram;