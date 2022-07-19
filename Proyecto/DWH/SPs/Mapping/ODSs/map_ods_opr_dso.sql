use MX_DWH
Go

if object_id ('etl.map_ods_opr_dso', 'P') is not null
	drop procedure etl.map_ods_opr_dso
GO

create procedure etl.map_ods_opr_dso (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 31-05-2022
	-- Description:	Mapping OPR DSO
	-- =============================================
	-- 24/06/2022 - LS - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @periodo varchar(6)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		begin transaction

		delete from dbo.ODS_OPR_DSO
		where periodo in (select distinct periodo from stg.ST_ODS_OPR_DSO )

		set @msg = 'Borrados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into dbo.ODS_OPR_DSO (periodo,
				cod_empresa, cod_sucursal, dso)
		select periodo,
			cod_empresa, cod_sucursal, dso
		from (select periodo, cod_empresa, cod_sucursal, dso,
				rank() over (partition by periodo, cod_empresa, cod_sucursal order by dso desc) as rnk
			from stg.ST_ODS_OPR_DSO ) vw
		where vw.rnk = 1
		order by 1,2,3
					
		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

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
-- exec etl.map_ods_opr_dso 0