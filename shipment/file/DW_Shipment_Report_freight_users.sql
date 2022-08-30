SELECT OrderType
      ,ShipmentID ,PreDONo ,[Delivery Order No] ,[PO Number]
      ,PERSPECTIVE ,DFM ,[Multi Stop Mark]
      ,[Shipment Status] ,[OrderStatus] ,[Shipping Condition] ,[Shipping Condition Description]
      ,[Shipping Type] ,[Shipping Type Description]
      ,[Logistics Region Group] ,[Logistics Region Code]
      ,[Transportation Zone] ,[Transportation Description]
      ,Province ,[Province Description]
      ,[BH Index]
      ,SoldToCode ,SoldToName ,ShipToCode ,ShipToName
      ,[Truck ID] ,Driver ,Plant ,ShippingPoint ,ShippingPointName
      --,[Weight In Qty] ,[Weight Out Qty]
      ,[Net Weight Qty] ,[Net Weight Qty by SH]
      ,[Logistics Material Group] ,Material ,[Material Description]
      ,[Transporter Code] ,[Transporter Name]
      ,[SO Create Date] ,[PreDO Create Date] ,[Shipment Create Date]
      ,[Request Date From] ,[Request Date To] ,[Log Confirm Delivery Date From] ,[Log Confirm Delivery Date To]
      ,[Assign Date] ,[Assign User]
      ,[Gate In Date] ,[Check In Date] ,[Weight In Date] ,[Load In Date] ,[Load Out Date]
      ,[Weight Out Date] ,[Weight Out Date by SH] ,[Gate Out Date]
      ,[Arrive At Cust Date] ,[Depart from Cust Date] ,[Logistics Confirm POD Date]
      ,Recycling ,[IO Log region group] ,DIVISION ,SALES_ORGANIZATION
      ,DIST_FROM_PREV_STOP ,DIST_FROM_MASTER ,LAT ,LON
      ,ref_shipment ,ORDER_RELEASE_LINE ,customer_type ,psling
      ,[Existing Order Alloc Cost] ,[Estimated Frieght Cost]
      -- ,[basefreight_otm] ,[accessorial_otm] ,[labor_otm]
      ,BaseFreight_TripFreight ,accessorial ,labor ,SuggestFreightCost ,BahtPerTon ,FinalFreightCost
      ,billing_date ,netweight_qty ,swap_cost ,swap_file ,mark_code
      ,unload_in_date ,unload_out_date ,tzone_source --,[actual_location]
      ,is_silo ,tzone_pk ,Delivery_note
FROM ShipmentTracking
WHERE 1=1
--AND [Weight Out Date] between '2021-06-16' and '2021-06-30 23:59:59' --[Weight Out Date] IS NULL --
--AND [Logistics Material Group] in ('BAG','BULK')
AND ShipmentID IN ('SH20210628-0129')
--AND [Delivery Order No] in ('Delivery Order No','3012669547','3012669547','3012701326')
--AND [Delivery Order No] in ('')
--AND [Logistics Material Group] in ('STEEL')
--AND [Transporter Code] in ('0002100491')
--AND [BH Index] in ('FAXXXX')
--AND ShippingPoint in ('0310001213')
 --And ShipToCode in ('0210036521')
