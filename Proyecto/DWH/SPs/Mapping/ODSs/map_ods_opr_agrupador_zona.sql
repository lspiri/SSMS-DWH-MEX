use MX_DWH
Go

if object_id ('etl.map_ods_opr_agrupador_zona', 'P') is not null
	drop procedure etl.map_ods_opr_agrupador_zona
GO

create procedure etl.map_ods_opr_agrupador_zona (
	@idBatch numeric(15), 
	@idMes numeric(10))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 26-05-2022
	-- Description:	Map ODS Agrupador x Zona
	-- =============================================
	-- 02/06/2022 - LS - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @idMesCarga numeric(10), @DescMes varchar(30),
			@cant int, @IdMesAnterior numeric(10)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @IdMesAnterior = 0

		begin transaction

			declare lc_cursor cursor for
			select a.id_mes, a.desc_nro_mes as mes, a.id_mes_ant
			from dbo.DTPO_LK_MESES a,
				dbo.DTPO_LK_MESES b
			where a.id_mes <= b.id_mes
				and a.anio = b.anio
				and b.id_mes = @idMes
			order by a.id_mes
		
			open lc_cursor
			fetch next from lc_cursor into @idMesCarga, @DescMes, @IdMesAnterior
			while @@fetch_status = 0
				begin

					set @msg = 'Mes: ' + @DescMes; exec etl.prc_Logging @idBatch, @msg

					delete from dbo.ODS_OPR_AGRUPADOR_ZONA
					where periodo = cast(@idMesCarga as varchar) 
				
					set @msg = 'Borrados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

					-- Origen Archivo
					insert into dbo.ODS_OPR_AGRUPADOR_ZONA (
						periodo, orden_present, 
						cod_cc, cod_agrupador, desc_cc, cod_zona, desc_zona, 
						dir_prorrateo, allocation, origen)
					select distinct periodo, orden_present, 
						cod_cc, cod_agrupador, descripcion, 
						isnull(cod_zona, 'NA') as cod_zona,
						desc_zona, 
						dir_prorrateo, allocation, 'Archivo'
					from stg.ST_ODS_OPR_AGRUPADOR_ZONA
					where periodo = cast(@idMesCarga as varchar) 
						and cod_cc is not null
						and cod_agrupador is not null

					set @cant = @@rowcount
					set @msg = 'Origen Archivo: ' + convert(varchar, @cant); exec etl.prc_Logging @idBatch, @msg
					
					-- Replica
					insert into dbo.ODS_OPR_AGRUPADOR_ZONA (
						periodo, orden_present, 
						cod_cc, cod_agrupador, desc_cc, cod_zona, desc_zona, 
						dir_prorrateo, allocation, origen)
					select @idMesCarga as Periodo, orden_present, 
						cod_cc, cod_agrupador, desc_cc, cod_zona, desc_zona, 
						dir_prorrateo, allocation, 'Replica ('+cast(@IdMesAnterior as varchar)+')'
					from dbo.ODS_OPR_AGRUPADOR_ZONA vw
					where vw.periodo = cast(@IdMesAnterior as varchar)
						and origen != 'Actual'
						and not exists (select 1 
							from dbo.ODS_OPR_AGRUPADOR_ZONA ods
							where ods.periodo = cast(@idMesCarga as varchar)
								and ods.cod_cc = vw.cod_cc)

					set @msg = 'Origen Replica ('+cast(@IdMesAnterior as varchar)+'): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

					-- LK del Actual
					insert into dbo.ODS_OPR_AGRUPADOR_ZONA (
						periodo, orden_present, 
						cod_cc, cod_agrupador, desc_cc, cod_zona, desc_zona, 
						dir_prorrateo, allocation, origen)

					select @idMesCarga, 0 as orden,
						vw.cod_cc, vw.cod_agrupador, vw.desc_cc, vw.cod_zona, vw.desc_zona,
						'NO' as dir_prorratero, null as allocation, 
						'Actual'
					from (select s.cod_sucursal as cod_cc,
							a.cod_agrupador as cod_agrupador,
							s.desc_sucursal as desc_cc,
							z.cod_zona,
							z.desc_zona
						from dbo.dgeo_lk_sucursal s,
							dbo.dgeo_lk_zona z,
							dbo.dgen_lk_agrupador_cc a
						where 1=1
							and s.id_agrupador = a.id_agrupador
							and s.id_zona = z.id_zona
							and s.id_sucursal > 0
							and s.id_agrupador > 0) vw
					where not exists (select 1 
						from dbo.ODS_OPR_AGRUPADOR_ZONA ods
						where ods.periodo = cast(@idMesCarga as varchar)
							and ods.cod_cc = vw.cod_cc)

					set @msg = 'Origen LKs Actual: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
					set @IdMesAnterior = @idMesCarga 
				
					-- next
					fetch next from lc_cursor into @idMesCarga, @DescMes, @IdMesAnterior
				end
			close lc_cursor
			deallocate lc_cursor

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
-- exec etl.map_ods_opr_agrupador_zona 0, 202204
