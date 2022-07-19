use MX_DWH
Go

if object_id ('etl.map_ods_opr_budget', 'P') is not null
	drop procedure etl.map_ods_opr_budget
GO

create procedure etl.map_ods_opr_budget (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 24-06-2022
	-- Description:	Mapping OPR Budget
	-- =============================================
	-- 24/06/2022 - LS - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @periodo varchar(6)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		begin transaction

		delete from dbo.ODS_OPR_BUDGET
		where periodo in (select distinct periodo from stg.ST_ODS_OPR_BUDGET )

		set @msg = 'Borrados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into dbo.ODS_OPR_BUDGET (periodo,
				tipo, cod_empresa, cod_sucursal, cod_cliente,
				cod_servicio, cod_cuenta, importe)
		select periodo,
			tipo, cod_empresa, cod_sucursal, cod_cliente,
			cod_servicio, cod_cuenta,
			sum(isnull(importe,0)) as importe
		from stg.ST_ODS_OPR_BUDGET
		group by periodo,
			tipo, cod_empresa, cod_sucursal, cod_cliente,
			cod_servicio, cod_cuenta
		having sum(isnull(importe,0)) <> 0

		set @msg = 'Insertados ODS: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

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
-- exec etl.map_ods_opr_budget 0