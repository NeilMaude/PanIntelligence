/*
	Extract of data for PanIntelligence Analytics testing

	Requires the dbo.WorkTime() function to be present - calculating working seconds between two datetime values

	NeilM, 16-Jul-2017

*/
USE [smart]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Drop and re-create the temp table for analysis data
DROP TABLE [dbo].[zTempPanAnalysis]
GO

CREATE TABLE [dbo].[zTempPanAnalysis](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Incident] [varchar](900) NOT NULL,
	[IncidentType] [varchar](20) NULL,
	[CustomerId] [varchar](20) NULL,
	[LedgerCode] varchar(10) NULL,
	[BusinessType] [varchar](50) NULL,
	[SiteId] [varchar](20) NULL,
	[MachinesOnSite] int NULL,
	[SoleMachine] bit NULL,
	[Manufacturer] [varchar] (25) NULL,
	[SerialNo] [varchar](40) NULL,
	[ProductId] [varchar](20) NULL,
	[DeviceType] [varchar](20) NULL,
	[Colour] bit NULL,
	[AncilliariesCount] int NULL,
	[FirstUsed] datetime NULL,
	[FirstUsedDays] int NULL,
	[Installed] datetime NULL,
	[InstalledDays] int NULL,
	[PreviousBreakdowns] int NULL,
	[LastBreakCall] datetime NULL,
	[LastBreakCallDays] int NULL,
	[LastOtherCall] datetime NULL,
	[LastOtherCallDays] int NULL,
	[LastOtherCallType] varchar(20) NULL,
	[InitialMeterValueBlack] int NULL,
	[InitialMeterValueColour] int NULL,
	[InitialMeterReadingDate] datetime NULL,
	[LastMeterValueBlack] int NULL,
	[LastMeterValueColour] int NULL,
	[LastMeterReadingDate] datetime NULL,
	[BlackClickPerDay] int NULL,
	[ColourClickPerDay] int NULL,
	[TotalClickPerDay] int NULL,
	[CreatedBy] [varchar](20) NULL,
	[CreatedDateTime] [datetime] NULL,
	[CreatedTime] [varchar](8) NULL,
	[CreatedDay] [nvarchar](30) NULL,
	[AttendDateTime] datetime NULL,
	[AttendDay] varchar(20) NULL,
	[AttendHour] int NULL,
	[CallMinutes] int NULL,
	[MinutesToAttend] int NULL,
	[PostCodeArea] [varchar](10) NULL,
	[FirstEngineer] [varchar](50) NULL

) ON [PRIMARY]
GO

-- function to get words
DROP FUNCTION [dbo].[zGetWord]
GO
CREATE FUNCTION [dbo].[zGetWord] 
    (
        @value varchar(max)
        , @startLocation int
    ) 
    RETURNS varchar(max) 
    AS 
      BEGIN 

         SET @value = LTRIM(RTRIM(@Value))  
         SELECT @startLocation = 
                CASE 
                    WHEN @startLocation > Len(@value) THEN LEN(@value) 
                    ELSE @startLocation 
                END

            SELECT @value = 
                CASE 
                    WHEN @startLocation > 1 
                        THEN LTRIM(RTRIM(RIGHT(@value, LEN(@value) - @startLocation)))
                    ELSE @value
                END

            RETURN CASE CHARINDEX(' ', @value, 1) 
                    WHEN 0 THEN @value 
                    ELSE SUBSTRING(@value, 1, CHARINDEX(' ', @value, 1) - 1) 
                END

     END 
GO

-- insert the base records

insert into zTempPanAnalysis

	(
	[Incident] ,
	[IncidentType] ,
	[CustomerId] ,
	[SiteId] ,
	[SerialNo] ,
	[ProductId] ,
	[CreatedBy] ,
	[CreatedDateTime] ,
	[CreatedTime] ,
	[CreatedDay] 
	)

select 

	incidentId									[Incident],
	incidentTypeId								[IncidentType],
	customerId									[CustomerId],
	siteId										[SiteId],
	serial										[SerialNo],
	productId									[ProductId],
	createdBy									[CreatedBy],
	createdDateTime								[CreatedDateTime],
	CONVERT(varchar(8), createdDateTime, 108)	[CreatedTime],
	datename(weekday, createdDateTime)			[CreatedDay]

	from dbo.incident 

	where incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT') and lastActivity = 'CLOSED'
GO

-- Now pick off the additional fields required

-- Type of machine
update zTempPanAnalysis set DeviceType = (select top 1 productGroup from contractline 
									where contractline.serial = zTempPanAnalysis.SerialNo
									and contractline.siteId = zTempPanAnalysis.siteId)
update zTempPanAnalysis set DeviceType = 'COPIER' where DeviceType = 'COP'
update zTempPanAnalysis set DeviceType = 'PRINTER' where DeviceType = 'PRI'
update zTempPanAnalysis set DeviceType = 'OTHER' where IsNull(DeviceType, '') Not In ('COPIER', 'PRINTER')
GO

-- Machines on the site
update zTempPanAnalysis set MachinesOnSite = (
	select count(*) from contractline 
	where startDate <= zTempPanAnalysis.CreatedDateTime and expiryDate >= zTempPanAnalysis.CreatedDateTime 
		 and contractline.siteId = zTempPanAnalysis.siteID and contractline.productGroup in ('COP','PRI'))
-- Now deal with any machines where there is no site contractline record
update zTempPanAnalysis set MachinesOnSite = 1 where MachinesOnSite < 1
GO

-- Sole machine?
update zTempPanAnalysis set SoleMachine = 0
update zTempPanAnalysis set SoleMachine = 1 where MachinesOnSite = 1
GO

-- Fix up any machines with missing serial numbers (some say 'NETWORK' or 'NETWORKING')
update zTempPanAnalysis Set SerialNo =  (select top 1 serial from contractline 
											where contractline.siteId = zTempPanAnalysis.SiteId
											and contractline.productId = zTempPanAnalysis.ProductId)
						where SerialNo Like 'NETWORK%' and MachinesOnSite = 1

-- First section of postcode e.g. LS1, WF1
update dbo.zTempPanAnalysis 
	set PostCodeArea = (select top 1 dbo.zGetWord(postcode,1) from dbo.site where dbo.site.siteId = dbo.zTempPanAnalysis.SiteId)
GO

-- First engineer to attend site
update dbo.zTempPanAnalysis 
set FirstEngineer = (select top 1 createdBy from dbo.incidenttransaction 
						where activityCodeId = 'ONSITE' 
							and dbo.incidenttransaction.incidentId = dbo.zTempPanAnalysis.Incident
						order by incidentTransactionId ASC)
update dbo.zTempPanAnalysis set FirstEngineer = 'UNKNOWN' where IsNull(FirstEngineer, 'NULL') = 'NULL'		-- deal with no engineer attended case
GO

-- Manufacturer
update zTempPanAnalysis set Manufacturer = (select top 1 manufacturerId from product where product.productId = zTempPanAnalysis.ProductId)
update zTempPanAnalysis set Manufacturer = 'UNKNOWN' where IsNull(Manufacturer, 'NULL') = 'NULL'
GO

-- Ancilliaries fitted count
update zTempPanAnalysis set AncilliariesCount = 
		(select count(*) from equipmentancillaryproduct 
			where equipmentancillaryproduct.equipmentId = zTempPanAnalysis.ProductId + '*' + zTempPanAnalysis.SerialNo)
GO

-- Colour machine?
update zTempPanAnalysis set Colour = (select Max(colourCostPerCopy) from contractline 
									where contractline.serial = zTempPanAnalysis.SerialNo
									and contractline.siteId = zTempPanAnalysis.siteId)
update zTempPanAnalysis set Colour = 0 where IsNull(Colour, 0) = 0
GO

-- Age of the machine (FirstUsed) and time in this site (Installed)
-- Note that in some cases Smart does not have an installation date or a first used date - so will have some zeros...
update zTempPanAnalysis set FirstUsed = (select Min(startDate) from contractline 
									where serial = zTempPanAnalysis.SerialNo
									and productId = zTempPanAnalysis.ProductId)
update zTempPanAnalysis set Installed = (select Min(startDate) from contractline 
									where serial = zTempPanAnalysis.SerialNo
									and productId = zTempPanAnalysis.ProductId
									and siteId = zTempPanAnalysis.SiteId)
update zTempPanAnalysis set FirstUsedDays = DATEDIFF(d,IsNull(FirstUsed, CreatedDateTime),CreatedDateTime)
update zTempPanAnalysis set InstalledDays = DATEDIFF(d,IsNull(Installed, CreatedDateTime),CreatedDateTime)
GO

-- Previous number of breakdowns
update zTempPanAnalysis set PreviousBreakdowns = (
		select count(*) from incident where incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and (lastActivity = 'CLOSED' or lastActivity = 'CANCELLED')
											and serial = zTempPanAnalysis.SerialNo
											and createdDateTime < zTempPanAnalysis.CreatedDateTime
											)
-- Zero those where there is no serial number...
update zTempPanAnalysis set PreviousBreakdowns = 0 where SerialNo Like 'NETWO%'
GO

-- Date of previous breakdown call, prior to incident
update zTempPanAnalysis set LastBreakCall = (
select max(createdDateTime) from incident where incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and (lastActivity = 'CLOSED' or lastActivity = 'CANCELLED')
											and serial = zTempPanAnalysis.SerialNo
											and createdDateTime < zTempPanAnalysis.CreatedDateTime)
update zTempPanAnalysis set LastBreakCallDays = DATEDIFF(d,IsNull(LastBreakCall, CreatedDateTime),CreatedDateTime)
GO

-- Date of previous other types of call
update zTempPanAnalysis set LastOtherCall = (
select max(createdDateTime) from incident where not incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and serial = zTempPanAnalysis.SerialNo
											and createdDateTime < zTempPanAnalysis.CreatedDateTime)
update zTempPanAnalysis set LastOtherCallDays = DATEDIFF(d,IsNull(LastOtherCall, CreatedDateTime),CreatedDateTime)
GO

-- Type of last non-breakdown call
update zTempPanAnalysis set LastOtherCallType = (
select incidentTypeId from incident where not incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and serial = zTempPanAnalysis.SerialNo
											and createdDateTime < zTempPanAnalysis.CreatedDateTime
											and createdDateTime = zTempPanAnalysis.LastOtherCall)
update zTempPanAnalysis set LastOtherCallType = 'NONE' where IsNull(LastOtherCall, getDate()) = getDate()		-- remove nulls
GO

-- Date and time of attending
update zTempPanAnalysis set AttendDateTime = (select min(createdDateTime) from incidenttransaction 
												where incidentId = zTempPanAnalysis.Incident
												and activityCodeId = 'ONSITE')
update zTempPanAnalysis set 
		AttendHour = Cast(Left(CONVERT(varchar(8), AttendDateTime, 108),2) As Int),
		AttendDay = datename(weekday, AttendDateTime)
update zTempPanAnalysis Set CallMinutes = DATEDIFF(n,
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'ONSITE'
												and incidentId = zTempPanAnalysis.Incident),
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'LEAVE'
												and incidentId = zTempPanAnalysis.Incident))
GO

-- Working time minutes, creation to first on-site
-- Note that this uses the dbo.WorkTime function to calculate times
update zTempPanAnalysis Set MinutesToAttend = dbo.WorkTime(
			zTempPanAnalysis.CreatedDateTime,
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'ONSITE'
												and incidentId = zTempPanAnalysis.Incident),
			'08:00', '17:00') / 60
GO

-- Set up meter readings and daily click
update zTempPanAnalysis Set InitialMeterValueBlack = (
	select top 1 reading from meterreading where serial = zTempPanAnalysis.SerialNo 
	and readingDateTime >= zTempPanAnalysis.FirstUsed and readingDateTime < zTempPanAnalysis.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4BLACK' order by readingDateTime ASC)
update zTempPanAnalysis Set InitialMeterValueColour = (
	select top 1 reading from meterreading where serial = zTempPanAnalysis.SerialNo 
	and readingDateTime >= zTempPanAnalysis.FirstUsed and readingDateTime < zTempPanAnalysis.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4COLOUR' order by readingDateTime ASC)
update zTempPanAnalysis Set InitialMeterReadingDate = (
	select top 1 readingDateTime from meterreading where serial = zTempPanAnalysis.SerialNo 
	and readingDateTime >= zTempPanAnalysis.FirstUsed and readingDateTime < zTempPanAnalysis.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4BLACK' order by readingDateTime ASC)
update zTempPanAnalysis Set LastMeterValueBlack = (
	select top 1 reading from meterreading where serial = zTempPanAnalysis.SerialNo 
	and readingDateTime >= zTempPanAnalysis.FirstUsed and readingDateTime < zTempPanAnalysis.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4BLACK' order by readingDateTime DESC)
update zTempPanAnalysis Set LastMeterValueColour = (
	select top 1 reading from meterreading where serial = zTempPanAnalysis.SerialNo 
	and readingDateTime >= zTempPanAnalysis.FirstUsed and readingDateTime < zTempPanAnalysis.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4COLOUR' order by readingDateTime DESC)
update zTempPanAnalysis Set LastMeterReadingDate = (
	select top 1 readingDateTime from meterreading where serial = zTempPanAnalysis.SerialNo 
	and readingDateTime >= zTempPanAnalysis.FirstUsed and readingDateTime < zTempPanAnalysis.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4BLACK' order by readingDateTime DESC)
GO
update zTempPanAnalysis Set BlackClickPerDay = (LastMeterValueBlack - InitialMeterValueBlack) / DATEDIFF(d, InitialMeterReadingDate, LastMeterReadingDate)
where DATEDIFF(d, InitialMeterReadingDate, LastMeterReadingDate) > 0
update zTempPanAnalysis Set ColourClickPerDay = (LastMeterValueColour - InitialMeterValueColour) / DATEDIFF(d, InitialMeterReadingDate, LastMeterReadingDate)
where DATEDIFF(d, InitialMeterReadingDate, LastMeterReadingDate) > 0
update zTempPanAnalysis set TotalClickPerDay = BlackClickPerDay + IsNull(ColourClickPerDay,0)
GO

-- Ledger Code
update zTempPanAnalysis set LedgerCode = (select top 1 ledgerAccount from dbo.customer where dbo.customer.customerId = zTempPanAnalysis.CustomerId)
GO

-- Match to Dynamics CRM for SIC code/sector (this requires Dynamics Account/Type in the relevant temp table)
update zTempPanAnalysis set BusinessType = (select top 1 Left(Business_Type,50) from zTempAccountBusinessType where LedgerCode = Account_Number)
GO
