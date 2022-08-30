Select [PreDONo]
FROM [dbo].[ShipmentTracking]
where [SALES_ORGANIZATION] in ('1002')
      and [Logistics Confirm POD Date] is not null
      and [Logistics Confirm POD Date] >= dbo.getlocaldate()-360
group by [PreDONo]
having count(*) = 1
