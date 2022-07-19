use MX_DWH
Go

if object_id ('etl.map_ods_deduccion_nomina_calc', 'P') is not null
	drop procedure etl.map_ods_deduccion_nomina_calc
GO

create procedure etl.map_ods_deduccion_nomina_calc (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 20-05-2022
	-- Description:	Calculo de Deducciones 
	-- =============================================
	-- 20/05/2022 - LS - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @cant int

		declare @id_proceso int, @nro_empleado numeric(20), @nro_nomina_proceso int,
			@minimo_garantizado float, @total_percepcion float,
			@concepto varchar(30), @importe float, @nro_orden int, @caracteristica varchar(30),
			@tipo_proceso varchar(10)
	
		declare @nuevo_neto float, @deduc_aplic float, @deduc_pend float, @orden int,
			@descuento  varchar(30), @desc_concepto varchar(100),
			@q_empleados int

		create table #deducciones (
			id_proceso numeric(10),
			nro_empleado numeric(10),
			nro_nomina_proceso int,
			tipo_deduccion varchar(30),
			orden int,
			caracteristica varchar(30),
			num_conc varchar(30),
			concepto varchar(100),
			importe float,
			descuento varchar(30),
			nuevo_neto float
		)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		declare lc_cursor cursor for
		select p.id as id_proceso, em.nro_empleado,
			em.nro_nomina_proceso,
			em.minimo_garantizado, em.total_percepcion,
			p.tipo_proceso
		from dbo.ODS_DN_PROCESO p
			left outer join dbo.ODS_DN_NOMINA_PROCESO em
				on p.id = em.id_proceso
		where 1=1
			and p.estado = 'PE'
		order by 1,3

		open lc_cursor
		fetch next from lc_cursor into @id_proceso, @nro_empleado, @nro_nomina_proceso, 
			@minimo_garantizado, @total_percepcion, @tipo_proceso
		while @@fetch_status = 0 
			begin
				print 'ID PROCESO ('+@tipo_proceso+'): ' + cast(@id_proceso as varchar) + ' - ' + cast(@nro_empleado as varchar)
				print '---------------------------'

				if @minimo_garantizado = 0 and @total_percepcion = 0
					begin
						print  '** NO TIENE SUELDO A PAGAR'
					end
				else
					begin
						set @nuevo_neto = @total_percepcion
						set @orden = 0

						declare lc_deduc cursor for
						select concepto, importe, nro_orden, caracteristica, desc_concepto
						from dbo.ODS_DN_NOMINA_CONCEPTO
						where id_proceso = @id_proceso
							and nro_empleado = @nro_empleado
							and nro_orden is not null
						order by nro_orden

						open lc_deduc
						fetch next from lc_deduc into @concepto, @importe, @nro_orden, @caracteristica, @desc_concepto
						while @@fetch_status = 0 
							begin

								set @orden = @orden + 1
								set @deduc_aplic = 0 
								set @deduc_pend = 0

								print '('+ cast(@orden as varchar)+') - ' + @caracteristica + ' - Conc: ' + @concepto

								-- Obligatorios
								if @caracteristica = 'Por Ley' or @caracteristica = '1' or @caracteristica = '4'
								begin
									set @nuevo_neto = @nuevo_neto - @importe 
									set @deduc_aplic = @importe
									set @descuento = 'COMPLETO'
								end

								-- Todo lo que se pueda deducir/se manda como pendiente de aplicar
								if @caracteristica = '2' or @caracteristica = '3'
								begin
									if @nuevo_neto <= @minimo_garantizado
										begin
											set @deduc_pend = @importe
											set @descuento = 'COMPLETO'
										end
									else if (@nuevo_neto - @importe) >= @minimo_garantizado
										begin 
											set @nuevo_neto = @nuevo_neto - @importe 
											set @deduc_aplic = @importe
											set @descuento = 'COMPLETO'
										end 
									else 
										begin
											set @deduc_aplic = @nuevo_neto - @minimo_garantizado
											set @nuevo_neto = @nuevo_neto - @deduc_aplic
											set @deduc_pend = @importe - @deduc_aplic
											set @descuento = 'PARCIAL'
										end
								end

								-- Todo o nada/se manda como pendiente de aplicar
								if @caracteristica = '5'
								begin
									if (@nuevo_neto - @importe) < @minimo_garantizado
										begin
											set @deduc_pend = @importe
											set @descuento = 'COMPLETO'
										end
									else 
										begin 
											set @nuevo_neto = @nuevo_neto - @importe 
											set @deduc_aplic = @importe
											set @descuento = 'COMPLETO'
										end 
								end

								print 'Nuevo Neto: '+ cast(@nuevo_neto as varchar)
								print 'Deduc. Aplic.: '+ cast(@deduc_aplic as varchar)
								print 'Deduc. Pend.: '+ cast(@deduc_pend as varchar)
					

								-- registro
								if @deduc_aplic <> 0
									insert into #deducciones (id_proceso, nro_empleado, nro_nomina_proceso, tipo_deduccion,
										orden, caracteristica, num_conc, importe,
										descuento, nuevo_neto, concepto)
									values (@id_proceso, @nro_empleado, @nro_nomina_proceso, 'APLICADAS', @orden, @caracteristica, @concepto,
										@deduc_aplic, @descuento, @nuevo_neto, @desc_concepto)

								if @deduc_pend <> 0
									insert into #deducciones (id_proceso, nro_empleado, nro_nomina_proceso, tipo_deduccion,
										orden, caracteristica, num_conc, importe,
										descuento, nuevo_neto, concepto)
									values (@id_proceso, @nro_empleado, @nro_nomina_proceso, 'PENDIENTE', @orden, @caracteristica, @concepto,
										@deduc_pend, @descuento, @nuevo_neto, @desc_concepto)

								-- next
								fetch next from lc_deduc into @concepto, @importe, @nro_orden, @caracteristica, @desc_concepto
							end
						close lc_deduc
						deallocate lc_deduc

						print '-----'
						print 'Nuevo Neto: ' + cast(@nuevo_neto as varchar)
					end

				-- next 
				print '---------------------------'
				fetch next from lc_cursor into @id_proceso, @nro_empleado, @nro_nomina_proceso, 
					@minimo_garantizado, @total_percepcion, @tipo_proceso
			end
		close lc_cursor
		deallocate lc_cursor	
		
		begin transaction

			delete ods 
			from dbo.ODS_DN_NOMINA_DEDUCCION ods
			where exists (select 1 from #deducciones d
				where d.id_proceso = ods.id_proceso)

			set @msg = 'Borrados: ' + cast(@@rowcount as varchar); exec etl.prc_Logging @idBatch, @msg

			insert into dbo.ODS_DN_NOMINA_DEDUCCION (
				id_proceso, nro_empleado, nro_nomina_proceso, tipo_deduccion, orden,
				caracteristica, concepto, desc_concepto, importe, descuento, nuevo_neto)
			select id_proceso, nro_empleado, nro_nomina_proceso, tipo_deduccion, orden,
				caracteristica, num_conc, concepto, importe, descuento, nuevo_neto
			from #deducciones

			set @cant = @@rowcount
			set @msg = 'Insertados: ' + cast(@cant as varchar); exec etl.prc_Logging @idBatch, @msg

		-- Resumen
		if @cant = 0
			begin
				set @msg = '** <b>NO SE REGISTRARON DEDUCCIONES</b>'; exec etl.prc_Logging @idBatch, @msg
			end
		else
			begin
				set @msg = 'Resumen: '; exec etl.prc_Logging @idBatch, @msg
				declare lc_cursor cursor for
				select id_proceso, count(distinct nro_empleado) as q_empleados,
					sum(case when tipo_deduccion = 'APLICADAS' then importe else 0 end) as deduc_aplic,
					sum(case when tipo_deduccion = 'PENDIENTE' then importe else 0 end) as deduc_pend
				from #deducciones
				group by id_proceso
				order by 1

				open lc_cursor
				fetch next from lc_cursor into @id_proceso, @q_empleados, @deduc_aplic, @deduc_pend
				while @@fetch_status = 0 
					begin
						exec etl.prc_Logging @idBatch, '--------'
						set @msg = 'Proceso: ' + cast(@id_proceso as varchar) + ' - Empleados: ' + cast(@q_empleados as varchar); exec etl.prc_Logging @idBatch, @msg
						set @msg = 'Deducciones Aplicadas: ' + replace(cast(@deduc_aplic as varchar), '.', ','); exec etl.prc_Logging @idBatch, @msg
						set @msg = 'Deducciones Pendientes: ' + replace(cast(@deduc_pend as varchar), '.', ','); exec etl.prc_Logging @idBatch, @msg

						-- Update Proceso
						update dbo.ODS_DN_PROCESO
						set estado = 'OK'
						where id = @id_proceso

						-- Update Empleados
						update ods
						set Nuevo_Neto = isnull(de.nuevo_neto,0),
							Deduc_Aplicadas = isnull(de.deduc_aplic,0),
							Deduc_Pendientes = isnull(de.deduc_pend,0)
						from dbo.ODS_DN_NOMINA_PROCESO ods
							left outer join (select id_proceso, d.nro_empleado,
									sum(case when tipo_deduccion = 'APLICADAS' then importe else 0 end) as deduc_aplic,
									sum(case when tipo_deduccion = 'PENDIENTE' then importe else 0 end) as deduc_pend,
									min(nuevo_neto) as nuevo_neto
								from dbo.ODS_DN_NOMINA_DEDUCCION d
								where id_proceso = @id_proceso
								group by d.id_proceso, d.nro_empleado) de
							on ods.id_proceso = de.id_proceso
								and ods.nro_empleado = de.nro_empleado
						where ods.id_proceso = @id_proceso

						-- next
						fetch next from lc_cursor into @id_proceso, @q_empleados, @deduc_aplic, @deduc_pend
					end
				close lc_cursor
				deallocate lc_cursor
			end

		commit transaction

		-- OK
		if @cant > 0
			return 1
		else
			return 0

	end try
	
	-- Error
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'MAPPING', @idBatch

		return -1

	end catch
end
GO
-- exec etl.map_ods_deduccion_nomina_calc 0