/* 
	Smart consumables view for PanIntelligence 
	Neil Maude
	17-Aug-2018
*/


USE [smart]
GO

/****** Object:  View [ReportsReader].[PI_Consumables2]    Script Date: 17/08/2018 16:05:22 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [ReportsReader].[PI_Consumables2] AS
SELECT
	i.incidentId,
	i.customerId,
	cu.ledgerAccount,
	i.serial,
	i.createdDateTime,
	so.productId,
	so.quantity,
	p.standardCost,
	CAST(ROUND(so.quantity * p.standardCost,2) as DECIMAL(10,2)) [order_value],			-- this makes the query slow...
	p.description,
	so.despatchedDateTime,
	cast(year(d.despatchDate) as varchar) + '-' + RIGHT('00'+cast(month(d.despatchDate) as varchar),2) [DespatchPeriod],
	pg.description [ConsumableType],
	IsNull(d.despatchTemplateId, 'UNKNOWN') [DespatchType]
FROM 
	[dbo].incident i
INNER JOIN
	[dbo].[stockorder] so ON i.incidentId = so.documentId		
INNER JOIN
	[dbo].product p ON p.productId = so.productId
INNER JOIN
	[dbo].productgroup pg ON p.productGroupId = pg.productGroupId
INNER JOIN 
	[dbo].despatch d ON so.despatchId = d.despatchId
LEFT JOIN
	[dbo].[customer] cu ON i.customerId = cu.customerId
WHERE
	incidentTypeId = 'CONSUMABLE' and lastActivity <> 'CANCELLED' and IsNull(d.despatchDate,'') <> ''

GO


