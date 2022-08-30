SELECT PreDONo, COUNT(*)
  FROM ShipmentTracking
  where 1=1
  and OrderType in ('SO-ECO')
  and PERSPECTIVE in ('B')
  group by PreDONo
  having COUNT(*) > 1
