USE [LA_CCPM]
GO

if OBJECT_ID('dbo.vw_mx_lk_contactos_sf', 'V') is not null
	drop view dbo.vw_mx_lk_contactos_sf
Go

create view dbo.vw_mx_lk_contactos_sf as
select cast( co.ContactId as varchar(255) )as ID_CONTACTO,
	cast( co.Account_Country_Code_PL__c as varchar(255) ) as COD_PAIS,
	cast( [Account.Name] as varchar(255) ) as CUENTA,
	replace( cast( co.National_Identifier__c as varchar(255)),'-','') as CUIT,
	cast( FirstName as varchar(255) ) as NOMBRE_CT,
	cast( LastName as varchar(255) ) as APELLIDO_CT,
	cast( Title as varchar(255) ) as TITULO_CT,
	cast( Function__c as varchar(255) ) as FUNCION,
	cast( Specific_Department_Name__c as varchar(255) ) as DEPARTAMENTO,
	cast( MailingStreet as varchar(255) ) as CALLE_CT,
	cast( MailingCity as varchar(255) ) as CIUDAD_CT,
	cast( MailingState as varchar(255) ) as PROVINCIA_CT,
	cast( MailingPostalCode as varchar(255) ) as COD_POSTAL_CT,
	cast( HomePhone as varchar(255) ) as TELEFONO_CT,
	cast( Fax as varchar(255) ) as FAX_CT,
	cast( MobilePhone as varchar(255) ) as CELULAR_CT,
	cast( Email as varchar(255) ) as EMAIL_CT,
	cast( Survey_Allowance_Given__c as varchar(255) ) as ACEPTA_PART_CT,
	cast( NULL as varchar(255) ) as CONTACTO,
	cast( [Owner.Name] as varchar(255) ) as PROPIETARIO
FROM tt_contacts co 
where co.Account_Country_Code_PL__c in ('MX')
GO



