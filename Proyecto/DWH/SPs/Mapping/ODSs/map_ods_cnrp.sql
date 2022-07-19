use MX_DWH
Go

if object_id ('etl.map_ods_cnrp', 'P') is not null
	drop procedure etl.map_ods_cnrp
GO

create procedure etl.map_ods_cnrp (
	@idBatch numeric(15),
	@idMes numeric(15))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 16-05-2022
	-- Description:	Mapping ODS CNRP
	-- =============================================
	-- 13/06/2022 - LS - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), @periodo varchar(6),
			@idMesDesde numeric(10), @anio_act numeric(4), @anio_ant numeric(10)

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		set @msg = 'Mes: ' + convert(varchar, @idMes); exec etl.prc_Logging @idBatch, @msg

		-- Parametros
		select @idMesDesde = a.anio_ant * 100 + 1,
			@anio_act = a.anio, 
			@anio_ant = a.anio_ant 
		from dbo.DTPO_LK_MESES m,
			dbo.DTPO_LK_ANIO a
		where m.anio = a.ANIO
			and m.id_mes = @idMes 

		set @msg = 'Mes Desde: ' + convert(varchar, @idMesDesde); exec etl.prc_Logging @idBatch, @msg				
		set @msg = 'Anio Anterior: ' + convert(varchar, @anio_ant); exec etl.prc_Logging @idBatch, @msg
		set @msg = 'Anio Actual: ' + convert(varchar, @anio_act); exec etl.prc_Logging @idBatch, @msg

		-- Servicio Especializado
		select cod_tipo_serv_detalle as cod_servicio
		into #serv_especializados
		from DFAC_LK_TIPO_SERVICIO_DETALLE sd,
			DFAC_LK_SERVICIO2 s2
		where sd.id_tipo_servicio2  =s2.id_tipo_servicio2
			and s2.desc_tipo_servicio2 = 'Serv. Especializado'

		select cod_cliente, cod_sucursal,  cod_servicio, cod_empresa,
			sum(venta) as venta, max(Ajuste) as Ajuste
		into #vta_anterior
		from (
			select cod_cliente, cod_sucursal,  cod_servicio, 'NA' as cod_empresa, sum(importe) as venta,
				0 as Ajuste
			from stg.ST_ODS_CNRP_VENTA ods
			where left(periodo,4) = cast(@anio_ant as varchar)
				and (
					(@anio_ant != 2021 and periodo >= cast(@idMesDesde as varchar) )
				or (
					@anio_ant = 2021 and (
						(periodo >= '202101'
							and not exists (select 1 from #serv_especializados s where s.cod_servicio = ods.cod_servicio) )
						or 
						(periodo >= '202109'
							and exists (select 1 from #serv_especializados s where s.cod_servicio = ods.cod_servicio) )
						)
					)
				)
			group by cod_cliente, cod_sucursal,  cod_servicio --, cod_empresa
			having sum(importe) != 0
			union all		
			select cod_cliente, cod_sucursal, cod_servicio, 'NA' as cod_empresa, sum(isnull(ods.importe,0)*-1) as venta,
				1 as Ajuste
			from dbo.ODS_OPR_AJUSTES_EG ods
			where left(ods.cod_cuenta,2) = ('60')
				and left(periodo,4) = cast(@anio_ant as varchar)
				and periodo >= cast(@idMesDesde as varchar)
			group by cod_cliente, cod_sucursal, cod_servicio --, cod_empresa
			having sum(importe) != 0 ) vw
		group by cod_cliente, cod_sucursal,  cod_servicio, cod_empresa
		having sum(venta) != 0

		set @msg = 'Vta. Anterior: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		select cod_cliente, cod_sucursal,  cod_servicio, cod_empresa,
			sum(venta) as venta, max(Ajuste) as Ajuste
		into #vta_actual
		from (
			select cod_cliente, cod_sucursal,  cod_servicio, 'NA' as cod_empresa, sum(importe) as venta,
				0 as Ajuste
			from stg.ST_ODS_CNRP_VENTA ods
			where left(periodo,4) = cast(@anio_act as varchar)
				and (
					(@anio_act != 2021 and periodo <= cast(@idMes as varchar) )
				or (
					@anio_act = 2021 and (
						(periodo between '202101' and cast(@idMes as varchar)
							and not exists (select 1 from #serv_especializados s where s.cod_servicio = ods.cod_servicio) )
						or 
						(periodo between '202109' and cast(@idMes as varchar)
							and exists (select 1 from #serv_especializados s where s.cod_servicio = ods.cod_servicio) )
						)
					)
				)
			group by cod_cliente, cod_sucursal,  cod_servicio --, cod_empresa
			having sum(importe) != 0
			union all		
			select cod_cliente, cod_sucursal, cod_servicio, 'NA' as cod_empresa, sum(isnull(ods.importe,0)*-1) as venta,
				1 as Ajuste
			from dbo.ODS_OPR_AJUSTES_EG ods
			where left(ods.cod_cuenta,2) = ('60')
				and left(periodo,4) = cast(@anio_act as varchar)
				and periodo <= cast(@idMes as varchar)
			group by cod_cliente, cod_sucursal, cod_servicio --, cod_empresa
			having sum(importe) != 0 ) vw
		group by cod_cliente, cod_sucursal,  cod_servicio, cod_empresa
		having sum(venta) != 0

		set @msg = 'Vta. Actual: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
		
		create unique index #vta_anterior_uk on #vta_anterior (cod_cliente, cod_sucursal,  cod_servicio, cod_empresa)
		create unique index #vta_actual_uk on #vta_actual (cod_cliente, cod_sucursal,  cod_servicio, cod_empresa)
		
		-- drop table #temporal
		select t.*, 
			isnull(ant.Ajuste,0) as Ajuste_Anterior,
			isnull(ant.venta,0) as Venta_Anterior,
			isnull(act.Ajuste,0) as Ajuste_Actual,
			isnull(act.venta,0) as Venta_Actual,
			'NA' as cod_condicion
		into #temporal
		from (
			select cod_cliente, cod_sucursal,  cod_servicio, cod_empresa
			from #vta_anterior
			union
			select cod_cliente, cod_sucursal,  cod_servicio, cod_empresa
			from #vta_actual) t
			left outer join #vta_anterior ant
				on ant.cod_cliente = t.cod_cliente
					and ant.cod_sucursal = t.cod_sucursal
					and ant.cod_servicio = t.cod_servicio
					and ant.cod_empresa = t.cod_empresa
			left outer join #vta_actual act
				on act.cod_cliente = t.cod_cliente
					and act.cod_sucursal = t.cod_sucursal
					and act.cod_servicio = t.cod_servicio
					and act.cod_empresa = t.cod_empresa

		set @msg = 'Temporal: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create unique index #temporal_uk on #temporal (cod_cliente, cod_sucursal,  cod_servicio, cod_empresa)

		set @msg = '- Calculos: '; exec etl.prc_Logging @idBatch, @msg

		-- Condiciones
		-- R: Tuvo Venta el Año Anterior y el Actual / Tubo vta Positiva
		update t set cod_condicion = 'R'
		-- select *
		from #temporal t
		where cod_condicion = 'NA'
			and (Venta_Anterior != 0 and Venta_Actual != 0) 

		set @msg = 'Retenidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
	
		-- N: Tiene Venta este Año y en el Actual No
		update t set cod_condicion = 'N'
		-- select *
		from #temporal t
		where cod_condicion = 'NA'
			and (Venta_Anterior = 0 and Venta_Actual != 0) 

		set @msg = 'Nuevos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- P: Tuvo Venta el Año Anterior y Este No
		update t set cod_condicion = 'P'
		-- select *
		from #temporal t
		where cod_condicion = 'NA'
			and (Venta_Anterior != 0 and Venta_Actual = 0) 

		set @msg = 'Perdidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		/*
		-- NA: Sin Clasificar
		select *
		from #temporal t
		where cod_condicion = 'NA'
			and (Venta_Anterior > 0 and Venta_Actual <= 0) 
		*/


		begin transaction

			delete from dbo.ODS_CNRP
			where periodo = cast(@idMes as varchar)

			set @msg = 'Borrados ODS: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

			insert into dbo.ODS_CNRP (periodo, cod_cliente, cod_sucursal, cod_servicio, 
				cod_empresa, cod_condicion_orig, cod_condicion_final, cod_condicion_anual,
				Venta_Anterior, Venta_Actual, Ajuste_Anterior, Ajuste_Actual,
				Excepcion)
			select cast(@idMes as varchar) as periodo, cod_cliente, cod_sucursal, cod_servicio, 
				cod_empresa, cod_condicion,  cod_condicion, cod_condicion,
				Venta_Anterior, Venta_Actual, Ajuste_Anterior, Ajuste_Actual,
				0 as Excepcion
			from #temporal t

			set @msg = 'Insertados ODS: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

			insert into dbo.ODS_CNRP (periodo, cod_cliente, cod_sucursal, cod_servicio, 
				cod_empresa, cod_condicion_orig, cod_condicion_final, cod_condicion_anual,
				Venta_Anterior, Venta_Actual, Ajuste_Anterior, Ajuste_Actual,
				Excepcion)
			select cast(@idMes as varchar) as periodo, cod_cliente, cod_sucursal, cod_servicio, 
				cod_empresa, estado as cod_condicion, estado as cod_condicion, estado as cod_condicion,
				0 as Venta_Anterior, 0 as Venta_Actual, 
				0 as Ajuste_Anterior, 0 as Ajuste_Actual,
				1 as excepcion
			from stg.ST_ODS_CNRP_EXCEPCION st
			where 1=1
				and año = left(cast(@idMes as varchar),4)
				and not exists (select 1 from dbo.ODS_CNRP ods
						where 1=1
							and ods.periodo = cast(@idMes as varchar)
							and ods.cod_cliente = st.cod_cliente
							and ods.cod_sucursal = st.cod_sucursal
							and ods.cod_servicio = st.cod_servicio
							and ods.cod_empresa = st.cod_empresa)

			set @msg = 'Insertados ODS (Excepcion): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

			update ods
			set cod_condicion_final = st.estado, cod_condicion_anual = st.estado, Excepcion = 1
			from dbo.ODS_CNRP ods, 
				stg.ST_ODS_CNRP_EXCEPCION st
			where 1=1
				and st.año = left(cast(@idMes as varchar),4)
				and ods.periodo = cast(@idMes as varchar)
				and ods.cod_cliente = st.cod_cliente
				and ods.cod_sucursal = st.cod_sucursal
				and ods.cod_servicio = st.cod_servicio
				and ods.cod_empresa = st.cod_empresa
				and ods.excepcion = 0

			set @msg = 'Actualizados ODS (Excepcion): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

			insert into dbo.ODS_CNRP (periodo, cod_cliente, cod_sucursal, cod_servicio, 
				cod_empresa, cod_condicion_orig, cod_condicion_final, cod_condicion_anual,
				Venta_Anterior, Venta_Actual, Ajuste_Anterior, Ajuste_Actual,
				Excepcion, Complemento)
			select cast(@idMes as varchar) as periodo, 
				vw.cod_cliente, vw.cod_sucursal, vw.cod_servicio, 
				vw.cod_empresa, vw.cod_condicion_orig, vw.cod_condicion_final, vw.cod_condicion_anual,
				0 as venta_anterior, 0 as venta_actual, 
				0 as Ajuste_Anterior, 0 as Ajuste_Actual,
				0 as excepcion, 1 as complemento
			from (
				select ods.*,
					Row_Number() OVER ( partition by cod_cliente, cod_sucursal, cod_servicio, cod_empresa Order By periodo desc) As rnk
				from dbo.ODS_CNRP ods,
					(select id_mes 
					from dbo.DTPO_LK_MESES m
					where anio = @anio_act
					and id_mes < @idMes) m
				where ods.periodo = cast(m.id_mes as varchar) 
					and cod_condicion_orig != 'NA'
					and cod_condicion_final != 'NA') vw
			where vw.rnk = 1
				and not exists (select 1
					from dbo.ODS_CNRP x
					where x.periodo = @idMes
						and x.cod_cliente = vw.cod_cliente
						and x.cod_sucursal = vw.cod_sucursal
						and x.cod_servicio = vw.cod_servicio
						and x.cod_empresa = vw.cod_empresa)

			set @msg = 'Insertados ODS (Complementos): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

			-- Reset
			update ods
			set retrospectiva = 0, cod_condicion_anual = cod_condicion_final
			from dbo.ODS_CNRP ods,
				(select id_mes 
				from dbo.DTPO_LK_MESES m
				where anio = @anio_act
				and id_mes < @idMes) m
			where ods.periodo = cast(m.id_mes as varchar) 
				and retrospectiva = 1
		
			-- Get Retrospectiva
			select b.periodo, b.cod_cliente, b.cod_sucursal, b.cod_servicio, b.cod_empresa, 
				a.cod_condicion_anual as cod_condicion_actual, 
				b.cod_condicion_anual as cod_condicion_anterior
			into #reprospectiva
			from (select cod_cliente, cod_sucursal, cod_servicio, cod_empresa, cod_condicion_anual
				from dbo.ODS_CNRP ods
				where periodo = @idMes) a,
				(select periodo, cod_cliente, cod_sucursal, cod_servicio, cod_empresa, cod_condicion_anual
				from dbo.ODS_CNRP ods,
					(select id_mes 
					from dbo.DTPO_LK_MESES m
					where anio = @anio_act
					and id_mes < @idMes) m
				where ods.periodo = cast(m.id_mes as varchar) ) b
			where a.cod_cliente = b.cod_cliente
				and a.cod_sucursal = b.cod_sucursal
				and a.cod_servicio = b.cod_servicio
				and a.cod_empresa = b.cod_empresa
				and a.cod_condicion_anual != b.cod_condicion_anual

			update ods
			set retrospectiva = 1, cod_condicion_anual = r.cod_condicion_actual
			from dbo.ODS_CNRP ods,
				#reprospectiva r
			where ods.periodo = r.periodo
				and ods.cod_cliente = r.cod_cliente
				and ods.cod_sucursal = r.cod_sucursal
				and ods.cod_servicio = r.cod_servicio
				and ods.cod_empresa = r.cod_empresa
				
			set @msg = 'Actualizados ODS (Reprospectiva): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

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

/*
exec etl.map_ods_cnrp 0, 202101
exec etl.map_ods_cnrp 0, 202102
exec etl.map_ods_cnrp 0, 202103
exec etl.map_ods_cnrp 0, 202104
exec etl.map_ods_cnrp 0, 202105
exec etl.map_ods_cnrp 0, 202106
exec etl.map_ods_cnrp 0, 202107
exec etl.map_ods_cnrp 0, 202108
exec etl.map_ods_cnrp 0, 202109
exec etl.map_ods_cnrp 0, 202110
exec etl.map_ods_cnrp 0, 202111
exec etl.map_ods_cnrp 0, 202112

exec etl.map_ods_cnrp 0, 202201
exec etl.map_ods_cnrp 0, 202202
exec etl.map_ods_cnrp 0, 202203
*/