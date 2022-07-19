use MX_DWH
Go

if object_id ('etl.map_ods_opr_ajustes', 'P') is not null
	drop procedure etl.map_ods_opr_ajustes
GO

create procedure etl.map_ods_opr_ajustes (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 02-05-2022
	-- Description:	Mapping OPR Ajustes
	-- =============================================
	-- 10/05/2022 - LS - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @periodo varchar(6)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		begin transaction

		delete from dbo.ODS_OPR_AJUSTES_EG
		where periodo in (select distinct periodo from stg.ST_ODS_OPR_AJUSTES_EG )

		set @msg = 'Borrados ODS (EG): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		delete from dbo.ODS_OPR_AJUSTES_CC
		where periodo in (select distinct periodo from stg.ST_ODS_OPR_AJUSTES_CC )

		set @msg = 'Borrados ODS (CC): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into dbo.ODS_OPR_AJUSTES_EG (periodo,
				tipo, cod_empresa, cod_sucursal, cod_cliente,
				cod_servicio, cod_cuenta, importe)
		select periodo,
			tipo, cod_empresa, cod_sucursal, cod_cliente,
			cod_servicio, cod_cuenta,
			sum(isnull(importe,0)) as importe
		from stg.ST_ODS_OPR_AJUSTES_EG
		where 1=1
		group by periodo,
			tipo, cod_empresa, cod_sucursal, cod_cliente,
			cod_servicio, cod_cuenta
		having sum(isnull(importe,0)) <> 0
		order by 1,2
					
		set @msg = 'Insertados ODS (EG): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into dbo.ODS_OPR_AJUSTES_CC (periodo,
				cod_cuenta, cod_sucursal)
		select periodo,
			cod_cuenta, 
			max(cod_sucursal) as cod_sucursal
		from stg.ST_ODS_OPR_AJUSTES_CC
		where 1=1
		group by periodo,
			cod_cuenta
		order by 1,2
					
		set @msg = 'Insertados ODS (CC): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		commit transaction

	end try
	
	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'MAPPING', @idBatch
	end catch
end
GO
-- exec etl.map_ods_opr_ajustes 0