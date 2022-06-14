use MX_DWH
go

if object_id('VW_ODS_DN_COMPA�IA', 'V') is not null
	drop view VW_ODS_DN_COMPA�IA
Go

create view dbo.VW_ODS_DN_COMPA�IA as
select NUM_CIA, COMPA�IA, SALARIO_MINIMO
from dbo.ODS_DN_CONFIG_SALARIO a
where periodo = (select max(periodo) 
		from dbo.ODS_DN_CONFIG_SALARIO b
		where b.num_cia = a.num_cia)
go
