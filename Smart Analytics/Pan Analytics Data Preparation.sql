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
	[FirstUsedDaysValid] bit NULL,
	[Installed] datetime NULL,
	[InstalledDays] int NULL,
	[InstalledDaysValid] bit NULL,
	[PreviousBreakdowns] int NULL,
	[LastBreakCall] datetime NULL,
	[LastBreakCallDays] int NULL,
	[LastBreakCallDaysValid] bit NULL,
	[LastOtherCall] datetime NULL,
	[LastOtherCallDays] int NULL,
	[LastOtherCallDaysValid] bit NULL,
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
	[BlackClickPerDayValid] bit NULL,
	[ColourClickPerDayValid] bit NULL,
	[TotalClickPerDayValid] bit NULL,
	[CreatedBy] [varchar](20) NULL,
	[CreatedDateTime] [datetime] NULL,
	[CreatedTime] [varchar](8) NULL,
	[CreatedDay] [nvarchar](30) NULL,
	[AttendDateTime] datetime NULL,
	[AttendDay] varchar(20) NULL,
	[AttendHour] int NULL,
	[AttendOnSite] bit NULL,
	[AttendTimeValid] bit NULL,
	[CallMinutes] int NULL,
	[MinutesToAttend] int NULL,
	[MinutesToAttendValid] bit NULL,
	[PostCodeArea] [varchar](10) NULL,
	[FirstEngineer] [varchar](50) NULL,
	[SymptomCodeId] [varchar](50) NULL,
	[SymptomDescription] [varchar](500) NULL

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

PRINT N'Temp table and function created.'

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
	[CreatedDay] ,
	[SymptomCodeId] ,
	[SymptomDescription]

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
	datename(weekday, createdDateTime)			[CreatedDay],
	symptomCodeId								[SymptomCodeId],
	symptomDescription							[SymptomDescription]

	from dbo.incident 

	where incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT') and lastActivity = 'CLOSED'
GO

PRINT N'Main data fields copied.'

-- Now pick off the additional fields required

-- Ledger Code
update zTempPanAnalysis set LedgerCode = (select top 1 ledgerAccount from dbo.customer where dbo.customer.customerId = zTempPanAnalysis.CustomerId)
GO
-- clean up the LedgerCode column
update zTempPanAnalysis Set CustomerId = dbo.zGetWord(CustomerId,1)
	where IsNull(LedgerCode, '') = ''
update zTempPanAnalysis Set LedgerCode = (select top 1 ledgerAccount from customer where customer.customerId = zTempPanAnalysis.CustomerId)
	where IsNull(LedgerCode, '') = ''
update zTempPanAnalysis Set LedgerCode = (select top 1 reportName from customer where customer.customerId = zTempPanAnalysis.CustomerId)
	where IsNull(LedgerCode, '') = ''
update zTempPanAnalysis Set LedgerCode = 'UNKNOWN'
	where IsNull(LedgerCode, '') = ''
GO

PRINT N'CustomerId cleaned and LedgerCode set.'

-- Type of machine
update zTempPanAnalysis set DeviceType = (select top 1 productGroup from contractline 
									where contractline.serial = zTempPanAnalysis.SerialNo
									and contractline.siteId = zTempPanAnalysis.siteId)
update zTempPanAnalysis set DeviceType = 'COPIER' where DeviceType = 'COP'
update zTempPanAnalysis set DeviceType = 'PRINTER' where DeviceType = 'PRI'
update zTempPanAnalysis set DeviceType = 'OTHER' where IsNull(DeviceType, '') Not In ('COPIER', 'PRINTER')
GO

PRINT N'Machine types updated.'

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

PRINT N'Machine counts updated.'

-- Fix up any machines with missing serial numbers (some say 'NETWORK' or 'NETWORKING')
update zTempPanAnalysis Set SerialNo =  (select top 1 serial from contractline 
											where contractline.siteId = zTempPanAnalysis.SiteId
											and contractline.productId = zTempPanAnalysis.ProductId)
						where SerialNo Like 'NETWORK%' and MachinesOnSite = 1
update zTempPanAnalysis Set SerialNo =  (select top 1 serial from contractline 
											where contractline.siteId = zTempPanAnalysis.SiteId
											and contractline.productId = zTempPanAnalysis.ProductId)
						where IsNull(SerialNo,'') = '' and MachinesOnSite = 1
update zTempPanAnalysis set SerialNo = 'UNKNOWN' where IsNull(SerialNo,'') = ''
PRINT N'Missing serial numbers fixed.'

-- First section of postcode e.g. LS1, WF1
update dbo.zTempPanAnalysis 
	set PostCodeArea = (select top 1 dbo.zGetWord(postcode,1) from dbo.site where dbo.site.siteId = dbo.zTempPanAnalysis.SiteId)
update zTempPanAnalysis set PostCodeArea = 'ZZ' where IsNull(PostCodeArea ,'') = ''			-- a couple of cases with NULL postcodes found
GO

PRINT N'Postcode regions extracted.'

-- First engineer to attend site
update dbo.zTempPanAnalysis 
set FirstEngineer = (select top 1 createdBy from dbo.incidenttransaction 
						where activityCodeId = 'ONSITE' 
							and dbo.incidenttransaction.incidentId = dbo.zTempPanAnalysis.Incident
						order by incidentTransactionId ASC)
update dbo.zTempPanAnalysis 
set FirstEngineer = (select top 1 createdBy from dbo.incidenttransaction 
						where activityCodeId = 'HELPDESK' 
							and dbo.incidenttransaction.incidentId = dbo.zTempPanAnalysis.Incident
						order by incidentTransactionId ASC)
					where IsNull(FirstEngineer, 'NULL') = 'NULL'
update dbo.zTempPanAnalysis set FirstEngineer = 'UNKNOWN' where IsNull(FirstEngineer, 'NULL') = 'NULL'		-- deal with no engineer attended case
GO

PRINT N'First attending engineer updated.'

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

PRINT N'Machine manufacturer/ancilliaries and colour capability updated.'

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
update zTempPanAnalysis set [FirstUsedDaysValid] = 0
update zTempPanAnalysis set [FirstUsedDaysValid] = 1 where IsNull(FirstUsed,'') <> ''
update zTempPanAnalysis set InstalledDays = DATEDIFF(d,IsNull(Installed, CreatedDateTime),CreatedDateTime)
update zTempPanAnalysis set [InstalledDaysValid] = 0
update zTempPanAnalysis set [InstalledDaysValid] = 1 where IsNull(Installed,'') <> ''
GO
-- Fix up any cases of FirstUsedDays or InstalledDays <= 0
-- This is in the main database, with contract starts later than the first service breakdown call...
update zTempPanAnalysis set [FirstUsedDaysValid] = 0 where FirstUsedDays < 0
update zTempPanAnalysis set [FirstUsedDays] = 0 where FirstUsedDays < 0
update zTempPanAnalysis set [InstalledDaysValid] = 0 where InstalledDays < 0
update zTempPanAnalysis set [InstalledDays] = 0 where InstalledDays < 0

PRINT N'Machine age and usage dates updated.'

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

PRINT N'Previous breakdown counts updated.'

-- Date of previous breakdown call, prior to incident
update zTempPanAnalysis set LastBreakCall = (
select max(createdDateTime) from incident where incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and (lastActivity = 'CLOSED' or lastActivity = 'CANCELLED')
											and serial = zTempPanAnalysis.SerialNo
											and createdDateTime < zTempPanAnalysis.CreatedDateTime)
update zTempPanAnalysis set LastBreakCallDays = DATEDIFF(d,IsNull(LastBreakCall, CreatedDateTime),CreatedDateTime)
update zTempPanAnalysis set [LastBreakCallDaysValid] = 0
update zTempPanAnalysis set [LastBreakCallDaysValid] = 1 where IsNull([LastBreakCall],'') <> ''
GO

-- Date of previous other types of call
update zTempPanAnalysis set LastOtherCall = (
select max(createdDateTime) from incident where not incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and serial = zTempPanAnalysis.SerialNo
											and createdDateTime < zTempPanAnalysis.CreatedDateTime)
update zTempPanAnalysis set LastOtherCallDays = DATEDIFF(d,IsNull(LastOtherCall, CreatedDateTime),CreatedDateTime)
update zTempPanAnalysis set [LastOtherCallDaysValid] = 0
update zTempPanAnalysis set [LastOtherCallDaysValid] = 1 where IsNull([LastOtherCall],'') <> ''
GO

-- Type of last non-breakdown call
update zTempPanAnalysis set LastOtherCallType = (
select top 1 incidentTypeId from incident where not incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and serial = zTempPanAnalysis.SerialNo
											and createdDateTime < zTempPanAnalysis.CreatedDateTime
											and createdDateTime = zTempPanAnalysis.LastOtherCall
											)
update zTempPanAnalysis set LastOtherCallType = 'NONE' where IsNull(LastOtherCall, getDate()) = getDate()		-- remove nulls
GO

PRINT N'Previous call information updated.'

-- Date and time of attending
update zTempPanAnalysis set AttendDateTime = (select min(createdDateTime) from incidenttransaction 
												where incidentId = zTempPanAnalysis.Incident
												and activityCodeId = 'ONSITE')
update zTempPanAnalysis set AttendOnSite = 0
update zTempPanAnalysis set AttendOnSite = 1 where IsNull(AttendDateTime,'') <> ''

update zTempPanAnalysis Set CallMinutes = DATEDIFF(n,
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'ONSITE'
												and incidentId = zTempPanAnalysis.Incident),
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'LEAVE'
												and incidentId = zTempPanAnalysis.Incident and createdDateTime > zTempPanAnalysis.AttendDateTime))
GO
-- If we did not attend, look for HELPDESK fixes
update zTempPanAnalysis set AttendDateTime = (select min(createdDateTime) from incidenttransaction 
												where incidentId = zTempPanAnalysis.Incident
												and activityCodeId = 'HELPDESK')
						where IsNull(AttendDateTime,'') = ''
update zTempPanAnalysis set 
		AttendHour = Cast(Left(CONVERT(varchar(8), AttendDateTime, 108),2) As Int),
		AttendDay = datename(weekday, AttendDateTime)
update zTempPanAnalysis set AttendTimeValid = 0
update zTempPanAnalysis set AttendTimeValid = 1 where IsNull(AttendDateTime,'') <> ''
-- Deal with AttendHour values which are clearly wrong - i.e. OnSite at midnight or similar
update zTempPanAnalysis set AttendTimeValid = 0 where AttendOnSite = 1 and AttendHour < 6
update zTempPanAnalysis set AttendTimeValid = 0 where AttendOnsite = 1 and AttendHour > 18

-- Fix up NULL CallMinutes, where we did not attend site...
update zTempPanAnalysis set CallMinutes = 0 where IsNull(CallMinutes,0) = 0

-- Fix up excessively long call times
update zTempPanAnalysis set CallMinutes = 1440 where CallMinutes > 1440


PRINT N'Attendance date/time set.'

-- Working time minutes, creation to first on-site
-- Note that this uses the dbo.WorkTime function to calculate times
update zTempPanAnalysis Set MinutesToAttend = dbo.WorkTime(
			zTempPanAnalysis.CreatedDateTime,
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'ONSITE'
												and incidentId = zTempPanAnalysis.Incident and createdDateTime > zTempPanAnalysis.CreatedDateTime),
			'08:00', '17:00') / 60
			where AttendOnSite = 1
update zTempPanAnalysis Set MinutesToAttend = dbo.WorkTime(
			zTempPanAnalysis.CreatedDateTime,
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'HELPDESK'
												and incidentId = zTempPanAnalysis.Incident and createdDateTime > zTempPanAnalysis.CreatedDateTime),
			'08:00', '17:00') / 60
			where AttendOnSite = 0
update zTempPanAnalysis Set MinutesToAttendValid = 0
update zTempPanAnalysis Set MinutesToAttendValid = 1 where IsNull(MinutesToAttend,-1) > 0
update zTempPanAnalysis Set MinutesToAttend = 0 where IsNull(MinutesToAttend,-1) < 0
GO

PRINT N'Attendance duration set.'

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

-- Fix up the click values and set the valid data marker fields
update zTempPanAnalysis set BlackClickPerDayValid = 0
update zTempPanAnalysis set BlackClickPerDayValid = 1 where IsNull(BlackClickPerDay, 0) > 0
update zTempPanAnalysis set BlackClickPerDay = 0 where IsNull(BlackClickPerDay, 0) <= 0
update zTempPanAnalysis set ColourClickPerDayValid = 0
update zTempPanAnalysis set ColourClickPerDayValid = 1 where IsNull(ColourClickPerDay, 0) > 0
update zTempPanAnalysis set ColourClickPerDay = 0 where IsNull(ColourClickPerDay, 0) <= 0
update zTempPanAnalysis set TotalClickPerDayValid = 0
update zTempPanAnalysis set TotalClickPerDayValid = 1 where IsNull(TotalClickPerDay, 0) > 0
update zTempPanAnalysis set TotalClickPerDay = 0 where IsNull(TotalClickPerDay, 0) <= 0

-- Fix up excessive mono click, where there are extra digits in the meterage values (suspect values)
update zTempPanAnalysis set BlackClickPerDayValid = 0 where BlackClickPerDay >=15000			-- arbitrary cut-off
update zTempPanAnalysis set ColourClickPerDayValid = 0 where ColourClickPerDay >=15000			
update zTempPanAnalysis set TotalClickPerDayValid = 0 where BlackClickPerDay >=15000 or ColourClickPerDay >=15000
update zTempPanAnalysis set BlackClickPerDay = 15000 where BlackClickPerDay >=15000
update zTempPanAnalysis set ColourClickPerDay = 15000 where ColourClickPerDay >=15000
update zTempPanAnalysis set TotalClickPerDay = BlackClickPerDay + ColourClickPerDay				-- just reset all of these

PRINT N'Meter readings and previous click rates set.'

-- Match to Dynamics CRM for SIC code/sector (this requires Dynamics Account/Type in the relevant temp table)
update zTempPanAnalysis set BusinessType = (select top 1 Left(Business_Type,50) from zTempAccountBusinessType where LedgerCode = Account_Number)
GO
update zTempPanAnalysis Set BusinessType = 'UNKNOWN'
	where IsNull(BusinessType, '') = ''
GO

PRINT N'Business types set.'

-- Tidy up some real junk values...
delete from zTempPanAnalysis where IsNull(CreatedBy,'') = ''
delete from zTempPanAnalysis where IsNull(AttendDateTime,'') = ''
update zTempPanAnalysis set SymptomCodeId = 'ZZ' where IsNull(SymptomCodeId,'') = ''
delete from zTempPanAnalysis where AttendDateTime < '31-Dec-1999'							-- 2 random rows with 1967 site attendance dates...
update zTempPanAnalysis set SymptomDescription = 'UNKNOWN' where IsNull(SymptomDescription,'') = ''

PRINT N'Extract complete.'