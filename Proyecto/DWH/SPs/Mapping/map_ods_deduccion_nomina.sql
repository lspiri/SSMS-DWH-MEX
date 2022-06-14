use MX_DWH
Go

if object_id ('etl.map_ods_deduccion_nomina', 'P') is not null
	drop procedure etl.map_ods_deduccion_nomina
GO

create procedure etl.map_ods_deduccion_nomina (
	@idBatch numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 19-05-2022
	-- Description:	Carga ODS para el Calculo
	-- =============================================
	-- 20/05/2022 - LS - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @cant_procesos int
		declare @id_proceso numeric(10), @num_cia numeric(10), @nro_proceso numeric(10), @tipo_proceso varchar(10), 
			@per_proceso numeric(10), @año_proceso numeric(10), @sem_proceso numeric(10), @mes_proceso numeric(10)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		begin transaction

		
		delete from dbo.ODS_DN_CONFIG_SALARIO 
		where periodo in (select distinct periodo from stg.ST_ODS_DN_CONFIG_SALARIO )

		set @msg = 'Borrados (Cfg. Salario): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into dbo.ODS_DN_CONFIG_SALARIO (periodo, 
			num_cia, compañia, salario_minimo)
		select periodo, 
			num_cia, compañia, salario_minimo
		from stg.ST_ODS_DN_CONFIG_SALARIO
		
		set @msg = 'Insertados ODS (Cfg. Salario): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		delete from dbo.ODS_DN_CONFIG_PERCEPCION
		where periodo in (select distinct periodo from stg.ST_ODS_DN_CONFIG_PERCEPCION )

		set @msg = 'Borrados (Cfg. Percepcion): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into dbo.ODS_DN_CONFIG_PERCEPCION (periodo,
			concepto, descripcion, suma_dias)
		select periodo, concepto, 
			max(descripcion) as descripcion,
			max(suma_dias) as suma_dias
		from stg.ST_ODS_DN_CONFIG_PERCEPCION 
		group by periodo, concepto

		set @msg = 'Insertados (Cfg. Percepcion): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg


		delete from dbo.ODS_DN_CONFIG_PRIORIDAD
		where periodo in (select distinct periodo from stg.ST_ODS_DN_CONFIG_PRIORIDAD)

		set @msg = 'Borrados (Cfg. Prioridad): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into dbo.ODS_DN_CONFIG_PRIORIDAD (
			periodo, nro_orden, concepto, 
			texto_cc_nomina, prioridad_deduccion, 
			caracteristica, regs_agrup)
		select periodo, nro_orden, concepto_human,
			max(texto_cc_nomina) as texto_cc_nomina,
			max(prioridad_deduc_moras) as prioridad_deduccion, 
			max(caracteristica_demora) as caracteristica,
			count(1) as regs_agrup
		from stg.ST_ODS_DN_CONFIG_PRIORIDAD
		where nro_orden is not null
			and concepto_human is not null
		group by periodo, nro_orden, concepto_human
		order by periodo, nro_orden

		set @msg = 'Insertados (Cfg. Prioridad): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		set @msg = '- Procesos: ' ; exec etl.prc_Logging @idBatch, @msg

		update dbo.ODS_DN_PROCESO
		set estado = 'CA'
		where estado = 'PE'

		set @msg = 'Reset Pendientes: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		set @cant_procesos = 0
		declare lc_cursor cursor for
		select isnull(p.id,0) as id_proceso, st.*
		from (select distinct st.num_cia,
				st.proceso as nro_proceso,
				st.tipo_empleado as tipo_proceso,
				st.num_per_proceso as per_proceso,
				st.anio_proceso as año_proceso,
				st.mes_contable_proceso as mes_proceso,	
				st.num_sem_proceso as sem_proceso	
			from stg.ST_ODS_DN_NOMINA_CONCEPTO st) st
				left outer join dbo.ODS_DN_PROCESO p
					on p.num_cia = st.num_cia
						and p.nro_proceso = st.nro_proceso
						and p.tipo_proceso = st.tipo_proceso
						and p.per_proceso = st.per_proceso
						and p.año_proceso = st.año_proceso
						and p.sem_proceso = st.sem_proceso
						and p.mes_proceso = st.mes_proceso

		open lc_cursor
		fetch next from lc_cursor into @id_proceso, @num_cia, @nro_proceso, 
			@tipo_proceso, @per_proceso, @año_proceso, @mes_proceso, @sem_proceso
		while @@fetch_status = 0
			begin

				if @id_proceso = 0
					begin
						insert into dbo.ODS_DN_PROCESO (num_cia, 
							nro_proceso, tipo_proceso, per_proceso,
							año_proceso, sem_proceso, mes_proceso, estado)
						values (@num_cia, @nro_proceso, @tipo_proceso, @per_proceso, 
						@año_proceso, @sem_proceso, @mes_proceso, 'PE')

						set @id_proceso = SCOPE_IDENTITY() 
					end
				else
					begin
						update dbo.ODS_DN_PROCESO
						set estado = 'PE'
						where id = @id_proceso
					end

				set @msg = 'Id Proceso: ' + convert(varchar, @id_proceso); exec etl.prc_Logging @idBatch, @msg

				delete from dbo.ODS_DN_NOMINA_PROCESO 
				where id_proceso = @id_proceso

				set @msg = 'Borrados (Empls.): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

				insert into dbo.ODS_DN_NOMINA_PROCESO (id_proceso, nro_empleado, nro_nomina_proceso, nombre_empleado, 
					paterno_empleado, materno_empleado, cant_dias, salario_minimo, total_percepcion, 
					minimo_garantizado, deduc_total, deduc_aplicadas, deduc_pendientes, nuevo_neto)
				select vw.id_proceso, vw.nro_empleado,
					row_number() over (order by id_proceso, nro_empleado) as nro_nomina_proceso,
					vw.nombre_empleado, vw.paterno_empleado, vw.materno_empleado,
					vw.cant_dias, vw.salario_minimo, vw.total_percepcion,
					case when vw.tipo_proceso = 'MF' then 0 else Cant_Dias * Salario_Minimo end as Minimo_Garantizado,
					cast(0 as float) as Deduc_Total,
					cast(0 as float) as Deduc_Aplicadas,
					cast(0 as float) as Deduc_Pendientes,
					cast(0 as float) as Nuevo_Neto
				from (
					select pr.id as id_proceso,
						em.numero_empleado as nro_empleado,
						max(em.paterno_empleado) as paterno_empleado,
						max(em.materno_empleado) as materno_empleado,
						max(em.nombre_empleado) as nombre_empleado,
						sum(case when isnull(pe.suma_dias,'') = 'Si' then isnull(em.dias,0)
							else 0 end
							) as Cant_Dias,
						round(max(isnull(sm.salario_minimo,0)),2) as Salario_Minimo,
						round(sum(case when pe.concepto is not null then isnull(em.importe,0) else 0 end),2) as Total_Percepcion,
						max(pr.tipo_proceso) as tipo_proceso
					from stg.ST_ODS_DN_NOMINA_CONCEPTO em
							left outer join dbo.ODS_DN_CONFIG_SALARIO sm
								on em.periodo = sm.periodo
									and em.num_cia = sm.num_cia
							left outer join dbo.ODS_DN_CONFIG_PERCEPCION pe
								on em.periodo = pe.periodo
									and em.concepto = pe.concepto,
						dbo.ODS_DN_PROCESO pr
					where 1=1
						and em.num_cia = pr.num_cia
						and em.proceso = pr.nro_proceso
						and em.tipo_empleado = pr.tipo_proceso
						and em.num_per_proceso = pr.per_proceso
						and em.anio_proceso = pr.año_proceso
						and em.num_sem_proceso = pr.sem_proceso
						and em.mes_contable_proceso = pr.mes_proceso		
						and pr.id = @id_proceso
					group by pr.id,
						em.numero_empleado) vw
				order by 2

				set @msg = 'Insertados (Empls.): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

				delete from dbo.ODS_DN_NOMINA_CONCEPTO 
				where id_proceso = @id_proceso

				set @msg = 'Borrados (Cptos.): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

				insert into dbo.ODS_DN_NOMINA_CONCEPTO (
					id_proceso, nro_empleado, concepto, desc_concepto, dias, importe,
					nro_orden, caracteristica, prioridad_deduccion)
				select @id_proceso as id_proceso,
					st.numero_empleado, 
					st.concepto, 
					max(descr_concepto) as desc_concepto, 
					sum(isnull(dias,0)) as dias, 
					sum(isnull(importe,0)) as importe,
					max(dp.Nro_Orden) as Nro_Orden, 
					max(dp.Caracteristica) as Caracteristica, 
					max(dp.Prioridad_Deduccion) as Prioridad_Deduccion
				from stg.ST_ODS_DN_NOMINA_CONCEPTO st
					left outer join dbo.ODS_DN_CONFIG_PRIORIDAD dp
					on dp.periodo = st.periodo
						and dp.Concepto = st.concepto
				
				where st.num_cia = @num_cia
					and st.proceso = @nro_proceso
					and st.tipo_empleado = @tipo_proceso
					and st.num_per_proceso = @per_proceso
					and st.anio_proceso = @año_proceso
					and st.num_sem_proceso = @sem_proceso
					and st.mes_contable_proceso = @mes_proceso
				group by st.numero_empleado, st.concepto

				set @msg = 'Insertados (Cpto.): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg


				-- Calc ToTal Deduc
				update c set Deduc_Total = d.deducciones
				from dbo.ODS_DN_NOMINA_PROCESO	c,
					(select d.id_proceso, d.nro_empleado, sum(d.importe) as deducciones 
					from dbo.ODS_DN_NOMINA_CONCEPTO d,
						dbo.ODS_DN_PROCESO p
					where d.id_proceso = p.id
						and p.id = @id_proceso
						and d.nro_orden is not null
					group by d.id_proceso, d.nro_empleado) d
				where c.id_proceso = d.id_proceso
					and c.nro_empleado = d.nro_empleado

				set @msg = 'Calc Total Deduc: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg


				set @cant_procesos = @cant_procesos + 1

				-- Next
				fetch next from lc_cursor into @id_proceso, @num_cia, @nro_proceso, 
					@tipo_proceso, @per_proceso, @año_proceso, @mes_proceso, @sem_proceso
			end
		close lc_cursor
		deallocate lc_cursor

		commit transaction

		-- OK
		return @cant_procesos 

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
-- exec etl.map_ods_deduccion_nomina 0