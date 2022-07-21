Use LA_CCPM
Go

if object_id ('dbo.vw_mx_ft_visitas_sf', 'V') is not null
	drop view dbo.vw_mx_ft_visitas_sf
GO

create view dbo.vw_mx_ft_visitas_sf as
SELECT cast(tow.CountryCode as varchar(30)) as COD_PAIS, 
	cast(tv.TECH_GUID__c as varchar(50)) as TECH_GUID,
	cast(left(TV.Id,15) as varchar(255)) as ID_ACTIVIDAD,
	cast(ts.Name as varchar(255)) AS SUCURSAL,
	ActivityDateTime AS FECHA, 
	cast(Type__c as varchar(255)) AS TIPO, 
	cast(isnull(subtipo,Subtype__c) as varchar(255)) AS SUBTIPO, 
	cast(tow.Email as varchar(255)) AS MAIL_USUARIO, 
	cast(tow.Name as varchar(255)) AS NOMBRE_USUARIO,
	cast(tc.National_Identifier__c as varchar(255)) AS CUIT_CUENTA, 
	cast(tc.Name as varchar(255)) AS NOMBRE_CUENTA, 
	cast(Service__c as varchar(255)) AS SERVICIO, 
	cast(Subject as varchar(255)) AS ASUNTO, 
	tv.CreatedDate AS FECHA_CREACION, 
	tv.LastModifiedDate AS FECHA_MODIF,
	cast(tcr.Name as varchar(255)) AS CREADOR_POR,
	cast(tow.Email as varchar(255)) AS  MAIL_CREADOR,
	tv.EndDateTime as FECHA_FIN,
	cast(ts.Cost_Centre__c as varchar(255)) as COD_SUCURSAL,
	cast(tc.TECH_GUID__c as varchar(255)) as GUID_CUENTA,
	cast(tc.VAT_Number__c as varchar(255)) as VAT_NUMBER
FROM tt_visitas_infov2 tv
	left join tt_sucursales ts on tv.branch__c=ts.id
	left join tt_cuentas tc on tv.accountid=tc.id
	left join tt_usuarios tow on tv.ownerid = tow.id
	left join tt_usuarios tcr on tv.createdbyid = tcr.id
	left join c_subtypes cs on tv.subtype__c=cs.subtype 
where 1=1
	AND tv.Country_PL__c='MX'
	AND tow.CountryCode IN ('MX')
	AND tow.IsActive=1
	AND Subtype__c NOT IN ('')
	AND (WhoId is not null OR  WhatId is not null)
	AND Convert(date, ActivityDateTime) BETWEEN DATEADD(mm, DATEDIFF(mm, 0, GETDATE()) -6, 0) AND DATEADD (dd, -1, DATEADD(mm, DATEDIFF(mm, 0, GETDATE()) + 1, 0))
	AND tv.EventSubtype='Event'
	AND tv.IsDeleted='0'
	and tc.Segmentation__c not in ('Large','Public Sector & Not Classified')
	and Type__c in ('Sales Visit','Admin-Service Visit')

-- select * from vw_mx_ft_visitas_sf where TECH_GUID = '1c03e98b-6cc4-4643-9246-fa2cd0255e55'