/*
	
	Contracted Machines-In-Field (MIF) by month

	Neil Maude

	21-Sept-2018

*/

USE smart

-- Create table to receive data
IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'zMIF_ByMonth'))
BEGIN
	DROP TABLE [dbo].[zMIF_ByMonth]
END
GO

CREATE TABLE [dbo].[zMIF_ByMonth](
       [Period] [varchar](10) NULL,
	   [Count] [Int] NULL,
       [ServiceType] [varchar](200) NULL,
	   [Manufacturer] [varchar](100) NULL,
	   [ServiceArea] [varchar](100) NULL
) ON [PRIMARY]
GO

DECLARE @startdate date
DECLARE @enddate date
Set @startdate = '1/Jan/2017'		-- date at which Smart started recording this data
Set @enddate = getdate()			-- we want to run the extract up to the current date

DECLARE @monthstartdate date

-- cursor to loop over months, up to the current date
DECLARE month_cursor CURSOR FOR
	SELECT  DATEADD(MONTH, x.number, @startdate) MonthDate
	FROM    master.dbo.spt_values x
	WHERE   x.type = 'P'        
	AND     x.number <= DATEDIFF(MONTH, @StartDate, @EndDate);

OPEN month_cursor  
FETCH NEXT FROM month_cursor INTO @monthstartdate

WHILE @@FETCH_STATUS = 0  
BEGIN 
	PRINT 'Processing for : ' + CONVERT(VARCHAR(10), @monthstartdate)

	INSERT INTO [dbo].[zMIF_ByMonth] ([Period], [Count], [ServiceType], [Manufacturer])
		select CONVERT(VARCHAR(10), @monthstartdate), count(*) [Count], servicelevelDesc [ServiceType], manufacturerId [Manufacturer]
		from dbo.contractline CL left join dbo.product PR on CL.productId = PR.productId
		where 
		IsNull(serial, '') <> ''									-- must be a serial item (takes out contract headers)
		and productGroup in ('COP', 'PRI')							-- only interested in copiers and printers, not scanners, faxes, IT kit etc...
		and IsNull(cancelledDateTime, GETDATE()) > @monthstartdate	-- must not be cancelled at this point
		and Cast(startDate as datetime) <= @monthstartdate			-- must have a contract which has started
		and Cast(expiryDate as datetime) >= @monthstartdate			-- must have a future expiry date (i.e. not already expired)
		and (not ledgerNo like 'S-S001%')							-- not Stratas acccounts 
		and (not serviceLevelDesc Like 'Time%')						-- exclude T&M
		and (not serviceLevelDesc Like '36%')						-- exclude 36 month warranty case
		and (not serviceLevelDesc Like 'Strat%')					-- exclude Stratas machines held in Arena stock/showroom
		and (not serviceLevelDesc Like '12 Month%')					-- exclude 12 month warranty
		and (not serviceLevelDesc Like 'OOA Out of Area Time%')		-- exclude OOA T&M
		group by CL.serviceLevelDesc, PR.manufacturerId

	FETCH NEXT FROM month_cursor INTO @monthstartdate
END

UPDATE dbo.zMIF_ByMonth set ServiceArea = 'Arena'
UPDATE dbo.zMIF_ByMonth set ServiceArea = 'Out-of-area' where UPPER(ServiceType) Like UPPER('%out of%')

CLOSE month_cursor
DEALLOCATE month_cursor


