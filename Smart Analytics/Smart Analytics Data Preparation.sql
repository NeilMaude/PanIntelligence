/*
	Extract of data for analytics testing

	Requires the dbo.WorkTime() function to be present - calculating working seconds between two datetime values

	This script is a copy of the work done for PanAnalytics testing, but repurposed to create zCALL_ANALYSIS

	NeilM, 2-Oct-2018

*/
USE [smart]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Create the logging table for this process
IF NOT (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'zCALL_ANALYSIS_LOG'))
BEGIN
	CREATE TABLE [dbo].[zCALL_ANALYSIS_LOG](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[DateTime] [datetime] NULL,
	[Description] [varchar](100) NULL
	) ON [PRIMARY]
	
	ALTER TABLE [dbo].[zCALL_ANALYSIS_LOG] ADD  CONSTRAINT [DF_zCALL_ANALYSIS_LOG_DateTime]  DEFAULT (getdate()) FOR [DateTime]

	GRANT SELECT, INSERT, UPDATE, DELETE ON [dbo].[zCALL_ANALYSIS_LOG] To ReportsReader			-- Needed for logging in the Python code...

END
GO

INSERT INTO [dbo].[zCALL_ANALYSIS_LOG] ([Description]) VALUES ('Start data extract process')

-- Drop and re-create the temp table for analysis data
IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'zCALL_ANALYSIS'))
BEGIN
	DROP TABLE [dbo].[zCALL_ANALYSIS]
END
GO

CREATE TABLE [dbo].[zCALL_ANALYSIS](
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
	[SymptomDescription] [varchar](500) NULL,
	[Repeated] [varchar](10) NULL
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

insert into zCALL_ANALYSIS

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

	where incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT') and lastActivity = 'CLOSED' and serial <> 'UNKNOWN' and productId <> 'NSFCHECK'
GO

PRINT N'Main data fields copied.'

-- Now pick off the additional fields required

-- Ledger Code
update zCALL_ANALYSIS set LedgerCode = (select top 1 ledgerAccount from dbo.customer where dbo.customer.customerId = zCALL_ANALYSIS.CustomerId)
GO
-- clean up the LedgerCode column
update zCALL_ANALYSIS Set CustomerId = dbo.zGetWord(CustomerId,1)
	where IsNull(LedgerCode, '') = ''
update zCALL_ANALYSIS Set LedgerCode = (select top 1 ledgerAccount from customer where customer.customerId = zCALL_ANALYSIS.CustomerId)
	where IsNull(LedgerCode, '') = ''
update zCALL_ANALYSIS Set LedgerCode = (select top 1 reportName from customer where customer.customerId = zCALL_ANALYSIS.CustomerId)
	where IsNull(LedgerCode, '') = ''
update zCALL_ANALYSIS Set LedgerCode = 'UNKNOWN'
	where IsNull(LedgerCode, '') = ''
GO

PRINT N'CustomerId cleaned and LedgerCode set.'

-- Type of machine
update zCALL_ANALYSIS set DeviceType = (select top 1 productGroup from contractline 
									where contractline.serial = zCALL_ANALYSIS.SerialNo
									and contractline.siteId = zCALL_ANALYSIS.siteId)
update zCALL_ANALYSIS set DeviceType = 'COPIER' where DeviceType = 'COP'
update zCALL_ANALYSIS set DeviceType = 'PRINTER' where DeviceType = 'PRI'
update zCALL_ANALYSIS set DeviceType = 'OTHER' where IsNull(DeviceType, '') Not In ('COPIER', 'PRINTER')
GO

PRINT N'Machine types updated.'

-- Machines on the site
update zCALL_ANALYSIS set MachinesOnSite = (
	select count(*) from contractline 
	where startDate <= zCALL_ANALYSIS.CreatedDateTime and expiryDate >= zCALL_ANALYSIS.CreatedDateTime 
		 and contractline.siteId = zCALL_ANALYSIS.siteID and contractline.productGroup in ('COP','PRI'))
-- Now deal with any machines where there is no site contractline record
update zCALL_ANALYSIS set MachinesOnSite = 1 where MachinesOnSite < 1
GO

-- Sole machine?
update zCALL_ANALYSIS set SoleMachine = 0
update zCALL_ANALYSIS set SoleMachine = 1 where MachinesOnSite = 1
GO

PRINT N'Machine counts updated.'

-- Fix up any machines with missing serial numbers (some say 'NETWORK' or 'NETWORKING')
update zCALL_ANALYSIS Set SerialNo =  (select top 1 serial from contractline 
											where contractline.siteId = zCALL_ANALYSIS.SiteId
											and contractline.productId = zCALL_ANALYSIS.ProductId)
						where SerialNo Like 'NETWORK%' and MachinesOnSite = 1
update zCALL_ANALYSIS Set SerialNo =  (select top 1 serial from contractline 
											where contractline.siteId = zCALL_ANALYSIS.SiteId
											and contractline.productId = zCALL_ANALYSIS.ProductId)
						where IsNull(SerialNo,'') = '' and MachinesOnSite = 1
update zCALL_ANALYSIS set SerialNo = 'UNKNOWN' where IsNull(SerialNo,'') = ''
PRINT N'Missing serial numbers fixed.'

-- First section of postcode e.g. LS1, WF1
update dbo.zCALL_ANALYSIS 
	set PostCodeArea = (select top 1 dbo.zGetWord(postcode,1) from dbo.site where dbo.site.siteId = dbo.zCALL_ANALYSIS.SiteId)
update zCALL_ANALYSIS set PostCodeArea = 'ZZ' where IsNull(PostCodeArea ,'') = ''			-- a couple of cases with NULL postcodes found
GO

PRINT N'Postcode regions extracted.'

-- First engineer to attend site
update dbo.zCALL_ANALYSIS 
set FirstEngineer = (select top 1 createdBy from dbo.incidenttransaction 
						where activityCodeId = 'ONSITE' 
							and dbo.incidenttransaction.incidentId = dbo.zCALL_ANALYSIS.Incident
						order by incidentTransactionId ASC)
update dbo.zCALL_ANALYSIS 
set FirstEngineer = (select top 1 createdBy from dbo.incidenttransaction 
						where activityCodeId = 'HELPDESK' 
							and dbo.incidenttransaction.incidentId = dbo.zCALL_ANALYSIS.Incident
						order by incidentTransactionId ASC)
					where IsNull(FirstEngineer, 'NULL') = 'NULL'
update dbo.zCALL_ANALYSIS set FirstEngineer = 'UNKNOWN' where IsNull(FirstEngineer, 'NULL') = 'NULL'		-- deal with no engineer attended case
GO

PRINT N'First attending engineer updated.'

-- Manufacturer
update zCALL_ANALYSIS set Manufacturer = (select top 1 manufacturerId from product where product.productId = zCALL_ANALYSIS.ProductId)
update zCALL_ANALYSIS set Manufacturer = 'UNKNOWN' where IsNull(Manufacturer, 'NULL') = 'NULL'
GO

-- Ancilliaries fitted count
update zCALL_ANALYSIS set AncilliariesCount = 
		(select count(*) from equipmentancillaryproduct 
			where equipmentancillaryproduct.equipmentId = zCALL_ANALYSIS.ProductId + '*' + zCALL_ANALYSIS.SerialNo)
GO

-- Colour machine?
update zCALL_ANALYSIS set Colour = (select Max(colourCostPerCopy) from contractline 
									where contractline.serial = zCALL_ANALYSIS.SerialNo
									and contractline.siteId = zCALL_ANALYSIS.siteId)
update zCALL_ANALYSIS set Colour = 0 where IsNull(Colour, 0) = 0
GO

PRINT N'Machine manufacturer/ancilliaries and colour capability updated.'

-- Age of the machine (FirstUsed) and time in this site (Installed)
-- Note that in some cases Smart does not have an installation date or a first used date - so will have some zeros...
update zCALL_ANALYSIS set FirstUsed = (select Min(startDate) from contractline 
									where serial = zCALL_ANALYSIS.SerialNo
									and productId = zCALL_ANALYSIS.ProductId)
update zCALL_ANALYSIS set Installed = (select Min(startDate) from contractline 
									where serial = zCALL_ANALYSIS.SerialNo
									and productId = zCALL_ANALYSIS.ProductId
									and siteId = zCALL_ANALYSIS.SiteId)
update zCALL_ANALYSIS set FirstUsedDays = DATEDIFF(d,IsNull(FirstUsed, CreatedDateTime),CreatedDateTime)
update zCALL_ANALYSIS set [FirstUsedDaysValid] = 0
update zCALL_ANALYSIS set [FirstUsedDaysValid] = 1 where IsNull(FirstUsed,'') <> ''
update zCALL_ANALYSIS set InstalledDays = DATEDIFF(d,IsNull(Installed, CreatedDateTime),CreatedDateTime)
update zCALL_ANALYSIS set [InstalledDaysValid] = 0
update zCALL_ANALYSIS set [InstalledDaysValid] = 1 where IsNull(Installed,'') <> ''
GO
-- Fix up any cases of FirstUsedDays or InstalledDays <= 0
-- This is in the main database, with contract starts later than the first service breakdown call...
update zCALL_ANALYSIS set [FirstUsedDaysValid] = 0 where FirstUsedDays < 0
update zCALL_ANALYSIS set [FirstUsedDays] = 0 where FirstUsedDays < 0
update zCALL_ANALYSIS set [InstalledDaysValid] = 0 where InstalledDays < 0
update zCALL_ANALYSIS set [InstalledDays] = 0 where InstalledDays < 0

PRINT N'Machine age and usage dates updated.'

-- Previous number of breakdowns
update zCALL_ANALYSIS set PreviousBreakdowns = (
		select count(*) from incident where incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and (lastActivity = 'CLOSED' or lastActivity = 'CANCELLED')
											and serial = zCALL_ANALYSIS.SerialNo
											and createdDateTime < zCALL_ANALYSIS.CreatedDateTime
											)
-- Zero those where there is no serial number...
update zCALL_ANALYSIS set PreviousBreakdowns = 0 where SerialNo Like 'NETWO%'
GO

PRINT N'Previous breakdown counts updated.'

-- Date of previous breakdown call, prior to incident
update zCALL_ANALYSIS set LastBreakCall = (
select max(createdDateTime) from incident where incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and (lastActivity = 'CLOSED' or lastActivity = 'CANCELLED')
											and serial = zCALL_ANALYSIS.SerialNo
											and createdDateTime < zCALL_ANALYSIS.CreatedDateTime)
update zCALL_ANALYSIS set LastBreakCallDays = DATEDIFF(d,IsNull(LastBreakCall, CreatedDateTime),CreatedDateTime)
update zCALL_ANALYSIS set [LastBreakCallDaysValid] = 0
update zCALL_ANALYSIS set [LastBreakCallDaysValid] = 1 where IsNull([LastBreakCall],'') <> ''
GO

-- Date of previous other types of call
update zCALL_ANALYSIS set LastOtherCall = (
select max(createdDateTime) from incident where not incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and serial = zCALL_ANALYSIS.SerialNo
											and createdDateTime < zCALL_ANALYSIS.CreatedDateTime)
update zCALL_ANALYSIS set LastOtherCallDays = DATEDIFF(d,IsNull(LastOtherCall, CreatedDateTime),CreatedDateTime)
update zCALL_ANALYSIS set [LastOtherCallDaysValid] = 0
update zCALL_ANALYSIS set [LastOtherCallDaysValid] = 1 where IsNull([LastOtherCall],'') <> ''
GO

-- Type of last non-breakdown call
update zCALL_ANALYSIS set LastOtherCallType = (
select top 1 incidentTypeId from incident where not incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT')
											and serial = zCALL_ANALYSIS.SerialNo
											and createdDateTime < zCALL_ANALYSIS.CreatedDateTime
											and createdDateTime = zCALL_ANALYSIS.LastOtherCall
											)
update zCALL_ANALYSIS set LastOtherCallType = 'NONE' where IsNull(LastOtherCall, getDate()) = getDate()		-- remove nulls
GO

PRINT N'Previous call information updated.'

-- Date and time of attending
update zCALL_ANALYSIS set AttendDateTime = (select min(createdDateTime) from incidenttransaction 
												where incidentId = zCALL_ANALYSIS.Incident
												and activityCodeId = 'ONSITE')
update zCALL_ANALYSIS set AttendOnSite = 0
update zCALL_ANALYSIS set AttendOnSite = 1 where IsNull(AttendDateTime,'') <> ''

update zCALL_ANALYSIS Set CallMinutes = DATEDIFF(n,
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'ONSITE'
												and incidentId = zCALL_ANALYSIS.Incident),
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'LEAVE'
												and incidentId = zCALL_ANALYSIS.Incident and createdDateTime > zCALL_ANALYSIS.AttendDateTime))
GO
-- If we did not attend, look for HELPDESK fixes
update zCALL_ANALYSIS set AttendDateTime = (select min(createdDateTime) from incidenttransaction 
												where incidentId = zCALL_ANALYSIS.Incident
												and activityCodeId = 'HELPDESK')
						where IsNull(AttendDateTime,'') = ''
update zCALL_ANALYSIS set 
		AttendHour = Cast(Left(CONVERT(varchar(8), AttendDateTime, 108),2) As Int),
		AttendDay = datename(weekday, AttendDateTime)
update zCALL_ANALYSIS set AttendTimeValid = 0
update zCALL_ANALYSIS set AttendTimeValid = 1 where IsNull(AttendDateTime,'') <> ''
-- Deal with AttendHour values which are clearly wrong - i.e. OnSite at midnight or similar
update zCALL_ANALYSIS set AttendTimeValid = 0 where AttendOnSite = 1 and AttendHour < 6
update zCALL_ANALYSIS set AttendTimeValid = 0 where AttendOnsite = 1 and AttendHour > 18

-- Fix up NULL CallMinutes, where we did not attend site...
update zCALL_ANALYSIS set CallMinutes = 0 where IsNull(CallMinutes,0) = 0

-- Fix up excessively long call times
update zCALL_ANALYSIS set CallMinutes = 1440 where CallMinutes > 1440


PRINT N'Attendance date/time set.'

-- Working time minutes, creation to first on-site
-- Note that this uses the dbo.WorkTime function to calculate times
update zCALL_ANALYSIS Set MinutesToAttend = dbo.WorkTime(
			zCALL_ANALYSIS.CreatedDateTime,
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'ONSITE'
												and incidentId = zCALL_ANALYSIS.Incident and createdDateTime > zCALL_ANALYSIS.CreatedDateTime),
			'08:00', '17:00') / 60
			where AttendOnSite = 1
update zCALL_ANALYSIS Set MinutesToAttend = dbo.WorkTime(
			zCALL_ANALYSIS.CreatedDateTime,
			(select min(createdDateTime) from incidenttransaction 
												where activityCodeId = 'HELPDESK'
												and incidentId = zCALL_ANALYSIS.Incident and createdDateTime > zCALL_ANALYSIS.CreatedDateTime),
			'08:00', '17:00') / 60
			where AttendOnSite = 0
update zCALL_ANALYSIS Set MinutesToAttendValid = 0
update zCALL_ANALYSIS Set MinutesToAttendValid = 1 where IsNull(MinutesToAttend,-1) > 0
update zCALL_ANALYSIS Set MinutesToAttend = 0 where IsNull(MinutesToAttend,-1) < 0
GO

PRINT N'Attendance duration set.'

-- Set up meter readings and daily click
update zCALL_ANALYSIS Set InitialMeterValueBlack = (
	select top 1 reading from meterreading where serial = zCALL_ANALYSIS.SerialNo 
	and readingDateTime >= zCALL_ANALYSIS.FirstUsed and readingDateTime < zCALL_ANALYSIS.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4BLACK' order by readingDateTime ASC)
update zCALL_ANALYSIS Set InitialMeterValueColour = (
	select top 1 reading from meterreading where serial = zCALL_ANALYSIS.SerialNo 
	and readingDateTime >= zCALL_ANALYSIS.FirstUsed and readingDateTime < zCALL_ANALYSIS.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4COLOUR' order by readingDateTime ASC)
update zCALL_ANALYSIS Set InitialMeterReadingDate = (
	select top 1 readingDateTime from meterreading where serial = zCALL_ANALYSIS.SerialNo 
	and readingDateTime >= zCALL_ANALYSIS.FirstUsed and readingDateTime < zCALL_ANALYSIS.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4BLACK' order by readingDateTime ASC)
update zCALL_ANALYSIS Set LastMeterValueBlack = (
	select top 1 reading from meterreading where serial = zCALL_ANALYSIS.SerialNo 
	and readingDateTime >= zCALL_ANALYSIS.FirstUsed and readingDateTime < zCALL_ANALYSIS.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4BLACK' order by readingDateTime DESC)
update zCALL_ANALYSIS Set LastMeterValueColour = (
	select top 1 reading from meterreading where serial = zCALL_ANALYSIS.SerialNo 
	and readingDateTime >= zCALL_ANALYSIS.FirstUsed and readingDateTime < zCALL_ANALYSIS.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4COLOUR' order by readingDateTime DESC)
update zCALL_ANALYSIS Set LastMeterReadingDate = (
	select top 1 readingDateTime from meterreading where serial = zCALL_ANALYSIS.SerialNo 
	and readingDateTime >= zCALL_ANALYSIS.FirstUsed and readingDateTime < zCALL_ANALYSIS.CreatedDateTime
	and IsNull(reading,0) > 0 and meterTypeId = 'A4BLACK' order by readingDateTime DESC)
GO
update zCALL_ANALYSIS Set BlackClickPerDay = (LastMeterValueBlack - InitialMeterValueBlack) / DATEDIFF(d, InitialMeterReadingDate, LastMeterReadingDate)
where DATEDIFF(d, InitialMeterReadingDate, LastMeterReadingDate) > 0
update zCALL_ANALYSIS Set ColourClickPerDay = (LastMeterValueColour - InitialMeterValueColour) / DATEDIFF(d, InitialMeterReadingDate, LastMeterReadingDate)
where DATEDIFF(d, InitialMeterReadingDate, LastMeterReadingDate) > 0
update zCALL_ANALYSIS set TotalClickPerDay = BlackClickPerDay + IsNull(ColourClickPerDay,0)
GO

-- Fix up the click values and set the valid data marker fields
update zCALL_ANALYSIS set BlackClickPerDayValid = 0
update zCALL_ANALYSIS set BlackClickPerDayValid = 1 where IsNull(BlackClickPerDay, 0) > 0
update zCALL_ANALYSIS set BlackClickPerDay = 0 where IsNull(BlackClickPerDay, 0) <= 0
update zCALL_ANALYSIS set ColourClickPerDayValid = 0
update zCALL_ANALYSIS set ColourClickPerDayValid = 1 where IsNull(ColourClickPerDay, 0) > 0
update zCALL_ANALYSIS set ColourClickPerDay = 0 where IsNull(ColourClickPerDay, 0) <= 0
update zCALL_ANALYSIS set TotalClickPerDayValid = 0
update zCALL_ANALYSIS set TotalClickPerDayValid = 1 where IsNull(TotalClickPerDay, 0) > 0
update zCALL_ANALYSIS set TotalClickPerDay = 0 where IsNull(TotalClickPerDay, 0) <= 0

-- Fix up excessive mono click, where there are extra digits in the meterage values (suspect values)
update zCALL_ANALYSIS set BlackClickPerDayValid = 0 where BlackClickPerDay >=15000			-- arbitrary cut-off
update zCALL_ANALYSIS set ColourClickPerDayValid = 0 where ColourClickPerDay >=15000			
update zCALL_ANALYSIS set TotalClickPerDayValid = 0 where BlackClickPerDay >=15000 or ColourClickPerDay >=15000
update zCALL_ANALYSIS set BlackClickPerDay = 15000 where BlackClickPerDay >=15000
update zCALL_ANALYSIS set ColourClickPerDay = 15000 where ColourClickPerDay >=15000
update zCALL_ANALYSIS set TotalClickPerDay = BlackClickPerDay + ColourClickPerDay				-- just reset all of these

PRINT N'Meter readings and previous click rates set.'

-- Match to Dynamics CRM for SIC code/sector (this requires Dynamics Account/Type in the relevant temp table)
update zCALL_ANALYSIS set BusinessType = (select top 1 Left(Business_Type,50) from zTempAccountBusinessType where LedgerCode = Account_Number)
GO
update zCALL_ANALYSIS Set BusinessType = 'UNKNOWN'
	where IsNull(BusinessType, '') = ''
GO

PRINT N'Business types set.'

-- Tidy up some real junk values...
delete from zCALL_ANALYSIS where IsNull(CreatedBy,'') = ''
delete from zCALL_ANALYSIS where IsNull(AttendDateTime,'') = ''
update zCALL_ANALYSIS set SymptomCodeId = 'ZZ' where IsNull(SymptomCodeId,'') = ''
delete from zCALL_ANALYSIS where AttendDateTime < '31-Dec-1999'							-- 2 random rows with 1967 site attendance dates...
update zCALL_ANALYSIS set SymptomDescription = 'UNKNOWN' where IsNull(SymptomDescription,'') = ''

-- Finally, update the 'Repeated' status
UPDATE zCALL_ANALYSIS Set Repeated = CAST(
									(select count(*) from dbo.incident 
										where 
											incident.customerId = zCALL_ANALYSIS.CustomerId
											and
											incident.productId = zCALL_ANALYSIS.ProductId
											and 
											incident.serial = zCALL_ANALYSIS.SerialNo
											and
											incident.createdDateTime > zCALL_ANALYSIS.AttendDateTime
											and
											incident.createdDateTime < DATEADD(d,14,zCALL_ANALYSIS.AttendDateTime)
											and
											incident.incidentId <> zCALL_ANALYSIS.Incident
											and
											incident.incidentTypeId in ('RTF', 'RTD', 'BREAKDOWN', 'REPEAT'))
									As varchar(30))
UPDATE zCALL_ANALYSIS Set Repeated = 'YES' Where Repeated <> '0'
UPDATE zCALL_ANALYSIS Set Repeated = 'NO' Where Repeated = '0'
UPDATE zCALL_ANALYSIS Set Repeated = 'MAYBE' Where Repeated = 'NO' and CreatedDateTime > DATEADD(d,-14,getdate())

INSERT INTO [dbo].[zCALL_ANALYSIS_LOG] ([Description]) VALUES ('End data extract process')

PRINT N'Extract complete.'