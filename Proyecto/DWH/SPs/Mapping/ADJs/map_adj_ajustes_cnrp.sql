use MX_DWH
Go

if object_id ('etl.map_adj_ajustes_cnrp', 'P') is not null
	drop procedure etl.map_adj_ajustes_cnrp
GO

create procedure etl.map_adj_ajustes_cnrp (
	@idBatch numeric(15), @idMes numeric(10) )
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date:	17/05/2022
	-- Description:	Mapping Ajustes Retrospectivos de CNRP
	-- =============================================
	-- 17/05/2022 - Version Inicial

	set nocount on

	declare @msg varchar(500)

	-- drop table #rel_ts
	create table #rel_ts (
		id_cliente numeric(10),
		id_sucursal numeric(10),
		id_tipo_serv_detalle numeric(10),
		id_mes numeric(10),
		cod_condicion varchar(30) COLLATE database_default,
		marca_clte_nuevo numeric(1) default 0,
		primary key (id_cliente, id_sucursal, id_tipo_serv_detalle, id_mes))

	-- drop table #rel_suc
	create table #rel_suc (
		id_cliente numeric(10),
		id_sucursal numeric(10),
		id_mes numeric(10),
		cod_condicion varchar(30) COLLATE database_default,
		primary key (id_cliente, id_sucursal, id_mes))

	-- drop table #rel_pais
	create table #rel_pais (
		id_cliente numeric(10),
		id_mes numeric(10),
		cod_condicion varchar(30) COLLATE database_default,
		primary key (id_cliente, id_mes))

	begin try

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg
		set @msg = 'Mes: ' + cast(@idMes as varchar); exec etl.prc_Logging @idBatch, @msg

		/*
		(select cast(periodo as numeric) as id_mes,
				etl.fn_LookupIdCliente(cod_cliente) as id_cliente, 
				etl.fn_LookupIdSucursal(cod_sucursal) as id_sucursal, 
				etl.fn_LookupIdTipoServDetalle(cod_servicio) as id_tipo_serv_detalle, 
				etl.fn_LookupIdCondicionCte (cod_condicion_anual) as id_condicion,
				cod_condicion_anual as cond_actual
			from dbo.ODS_CNRP 
			where periodo = @idMes) rel,
		*/

		select rel.*
		into #casos_ts
		from (select rel.id_mes,
				rel.id_cliente, 
				rel.id_sucursal, 
				rel.id_tipo_serv_detalle, 
				rel.id_condicion,
				con.cod_condicion as cond_actual
			from dbo.REL_CNRP_TS rel,
				dbo.DCTE_LK_CONDICION con
			where rel.id_condicion = con.id_condicion
				and rel.id_mes = @idMes) rel,
			(select rel.id_cliente, rel.id_sucursal, rel.id_tipo_serv_detalle 
			from dbo.REL_CNRP_TS rel,
				(select id_mes 
				from dbo.DTPO_LK_MESES m
				where anio = left(cast(@idMes as varchar),4)
				and id_mes <= @idMes) m
			where rel.id_mes = m.id_mes
			group by rel.id_cliente, rel.id_sucursal, rel.id_tipo_serv_detalle
			having count(distinct id_condicion) > 1) vw
		where rel.id_cliente = vw.id_cliente
			and rel.id_sucursal = vw.id_sucursal
			and rel.id_tipo_serv_detalle = vw.id_tipo_serv_detalle

		set @msg = 'Casos (TS): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		select distinct rel.*
		into #casos_suc
		from (select rel.id_mes,
				rel.id_cliente, 
				rel.id_sucursal, 
				etl.fn_LookupIdTipoServDetalle( null ) as id_tipo_serv_detalle, 
				rel.id_condicion,
				con.cod_condicion as cond_actual
			from dbo.REL_CNRP_SUCURSAL rel,
				dbo.DCTE_LK_CONDICION con
			where rel.id_condicion = con.id_condicion
				and rel.id_mes = @idMes) rel,
			(select rel.id_cliente, rel.id_sucursal
			from dbo.REL_CNRP_SUCURSAL rel,
				(select id_mes 
				from dbo.DTPO_LK_MESES m
				where anio = left(cast(@idMes as varchar),4)
					and id_mes <= @idMes) m
			where rel.id_mes = m.id_mes
			group by rel.id_cliente, rel.id_sucursal
			having count(distinct id_condicion) > 1) vw
		where rel.id_cliente = vw.id_cliente
			and rel.id_sucursal = vw.id_sucursal
			
		-- Determinar Estado Actual	(Suc)	
		select vw.*
		into #casos_suc_final
		from (
			select a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
				max(id_condicion) as id_condicion, max(cond_actual) as cond_actual
			from #casos_suc a
			where a.cond_actual <> 'NA'
			group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle
			having count(distinct cond_actual) = 1 
			union
			select a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
				etl.fn_LookupIdCondicionCte ('R') as id_condicion, 'R' as cond_actual
			from #casos_suc a
			where a.cond_actual <> 'NA'
				and exists (select 1 
						from #casos_suc b
						where b.id_cliente = a.id_cliente
							and b.id_sucursal = a.id_sucursal
							and b.id_mes = a.id_mes
							and b.cond_actual = 'R')
			group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle
			having count(distinct cond_actual) > 1 
			union
			select a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
				etl.fn_LookupIdCondicionCte ('N') as id_condicion, 'N' as cond_actual
			from #casos_suc a
			where a.cond_actual <> 'NA'
				and not exists (select 1 
						from #casos_suc b
						where b.id_cliente = a.id_cliente
							and b.id_sucursal = a.id_sucursal
							and b.id_mes = a.id_mes
							and b.cond_actual = 'R')
			group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle
			having count(distinct cond_actual) > 1  ) vw
		order by 1,2,3
		
		set @msg = 'Casos (SUC): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg


		select distinct rel.*
		into #casos_pais
		from (select rel.id_mes,
				rel.id_cliente, 
				etl.fn_LookupIdSucursal( null ) as id_sucursal, 
				etl.fn_LookupIdTipoServDetalle( null ) as id_tipo_serv_detalle, 
				rel.id_condicion,
				con.cod_condicion as cond_actual
			from dbo.REL_CNRP_TS rel,
				dbo.DCTE_LK_CONDICION con
			where rel.id_condicion = con.id_condicion
				and rel.id_mes = @idMes) rel,
			(select rel.id_cliente
			from dbo.REL_CNRP_PAIS rel,
				(select id_mes 
				from dbo.DTPO_LK_MESES m
				where anio = left(cast(@idMes as varchar),4)
					and id_mes <= @idMes) m
			where rel.id_mes = m.id_mes
			group by rel.id_cliente
			having count(distinct id_condicion) > 1) vw
		where rel.id_cliente = vw.id_cliente

		-- Determinar Estado Actual (Pais)
		select vw.*
		into #casos_pais_final
		from (
			select a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
				max(id_condicion) as id_condicion, max(cond_actual) as cond_actual
			from #casos_pais a
			where a.cond_actual <> 'NA'
			group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle
			having count(distinct cond_actual) = 1 
			union
			select a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
				etl.fn_LookupIdCondicionCte ('R') as id_condicion, 'R' as cond_actual
			from #casos_pais a
			where a.cond_actual <> 'NA'
				and exists (select 1 
						from #casos_pais b
						where b.id_cliente = a.id_cliente
							and b.id_mes = a.id_mes
							and b.cond_actual = 'R')
			group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle
			having count(distinct cond_actual) > 1 
			union
			select a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
				etl.fn_LookupIdCondicionCte ('N') as id_condicion, 'N' as cond_actual
			from #casos_pais a
			where a.cond_actual <> 'NA'
				and not exists (select 1 
						from #casos_pais b
						where b.id_cliente = a.id_cliente
							and b.id_mes = a.id_mes
							and b.cond_actual = 'R')
			group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle
			having count(distinct cond_actual) > 1  ) vw
		order by 1,2,3
		
		set @msg = 'Casos (PAIS): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		begin transaction

		delete PD 
		from stg.PD_AJUSTES_CNRP PD
		where PD.id_mes = @idMes
			and PD.nivel_ajuste = 'REL_CNRP_TS'
			and exists (select 1 from #casos_ts c
					where c.id_cliente = PD.id_cliente	
						and c.id_sucursal = PD.id_sucursal
						and c.id_tipo_serv_detalle = PD.id_tipo_serv_detalle)

		set @msg = 'Del Ajustes (TS): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into stg.PD_AJUSTES_CNRP (
			id_mes, nivel_ajuste, id_cliente, id_sucursal, id_tipo_serv_detalle,
			id_condicion, cond_actual, id_mes_anterior, cond_anterior)
		select a.id_mes, 'REL_CNRP_TS', a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
			a.id_condicion, a.cond_actual, b.id_mes as id_mes_anterior, b.cond_anterior
		from #casos_ts a,
			(select rel.id_mes, rel.id_cliente, rel.id_sucursal, rel.id_tipo_serv_detalle, 
				con.cod_condicion as cond_anterior
			from dbo.REL_CNRP_TS rel,
				(select id_mes 
				from dbo.DTPO_LK_MESES m
				where anio = left(cast(@idMes as varchar),4)
				and id_mes < @idMes) m,
				dbo.DCTE_LK_CONDICION con
			where rel.id_mes = m.id_mes
				and rel.id_condicion = con.id_condicion) b
		where a.id_cliente = b.id_cliente
			and a.id_sucursal = b.id_sucursal
			and a.id_tipo_serv_detalle = b.id_tipo_serv_detalle
			and a.cond_actual != b.cond_anterior
			and a.cond_actual not in ('NA')
			-- and a.cond_actual in ('R', 'P')

		set @msg = 'Ins Ajustes (TS): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		delete PD 
		from stg.PD_AJUSTES_CNRP PD
		where PD.id_mes = @idMes
			and PD.nivel_ajuste = 'REL_CNRP_SUCURSAL'
			and exists (select 1 from #casos_suc_final c
					where c.id_cliente = PD.id_cliente	
						and c.id_sucursal = PD.id_sucursal)

		set @msg = 'Del Ajustes (SUC): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into stg.PD_AJUSTES_CNRP (
			id_mes, nivel_ajuste, id_cliente, id_sucursal, id_tipo_serv_detalle,
			id_condicion, cond_actual, id_mes_anterior, cond_anterior)
		select a.id_mes, 'REL_CNRP_SUCURSAL', a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
			a.id_condicion, a.cond_actual, b.id_mes as id_mes_anterior, b.cond_anterior
		from #casos_suc_final a,
			(select rel.id_mes, rel.id_cliente, rel.id_sucursal, 
				con.cod_condicion as cond_anterior
			from dbo.REL_CNRP_SUCURSAL rel,
				(select id_mes 
				from dbo.DTPO_LK_MESES m
				where anio = left(cast(@idMes as varchar),4)
				and id_mes < @idMes) m, 
				dbo.DCTE_LK_CONDICION con
			where rel.id_mes = m.id_mes
				and rel.id_condicion = con.id_condicion) b
		where a.id_cliente = b.id_cliente
			and a.id_sucursal = b.id_sucursal
			and a.cond_actual != b.cond_anterior
			and a.cond_actual not in ('NA')

		set @msg = 'Ins Ajustes (SUC): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		delete PD 
		from stg.PD_AJUSTES_CNRP PD
		where PD.id_mes = @idMes
			and PD.nivel_ajuste = 'REL_CNRP_PAIS'
			and exists (select 1 from #casos_pais_final c
					where c.id_cliente = PD.id_cliente)

		set @msg = 'Del Ajustes (PAIS): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into stg.PD_AJUSTES_CNRP (
			id_mes, nivel_ajuste, id_cliente, id_sucursal, id_tipo_serv_detalle,
			id_condicion, cond_actual, id_mes_anterior, cond_anterior)
		select a.id_mes, 'REL_CNRP_PAIS', a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
			a.id_condicion, a.cond_actual, b.id_mes as id_mes_anterior, b.cond_anterior
		from #casos_pais_final a,
			(select rel.id_mes, rel.id_cliente, 
				con.cod_condicion as cond_anterior
			from dbo.REL_CNRP_PAIS rel,
				(select id_mes 
				from dbo.DTPO_LK_MESES m
				where anio = left(cast(@idMes as varchar),4)
				and id_mes <= @idMes) m, -- Incluye el Mes Actual (Algo queda mal en Rel CNRP)
				dbo.DCTE_LK_CONDICION con
			where rel.id_mes = m.id_mes
				and rel.id_condicion = con.id_condicion) b
		where a.id_cliente = b.id_cliente
			and a.cond_actual != b.cond_anterior
			and a.cond_actual not in ('NA')

		set @msg = 'Ins Ajustes (PAIS): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-----------------------------
		-- Relacion Tipo Servicio (TS)
		----------------------------
		update rel
		set rel.id_condicion = a.id_condicion
		from dbo.REL_CNRP_TS rel,
			stg.PD_AJUSTES_CNRP a
		where rel.id_mes = a.id_mes_anterior
			and rel.id_cliente = a.id_cliente
			and rel.id_sucursal = a.id_sucursal
			and rel.id_tipo_serv_detalle = a.id_tipo_serv_detalle 
			and rel.id_condicion != a.id_condicion
			and a.id_mes = @idMes
			and a.nivel_ajuste = 'REL_CNRP_TS'

		set @msg = 'Updates Retrospectivo (TS): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
		
		-----------------------------
		-- Relacion Sucursal (SUC)
		----------------------------
		update rel
		set rel.id_condicion = a.id_condicion
		from dbo.REL_CNRP_SUCURSAL rel,
			stg.PD_AJUSTES_CNRP a
		where rel.id_mes = a.id_mes_anterior
			and rel.id_cliente = a.id_cliente
			and rel.id_sucursal = a.id_sucursal
			and rel.id_condicion != a.id_condicion
			and a.id_mes = @idMes
			and a.nivel_ajuste = 'REL_CNRP_SUCURSAL'

		set @msg = 'Updates Retrospectivo (SUC): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		update rel
		set rel.id_condicion = a.id_condicion
		from dbo.REL_CNRP_PAIS rel,
			stg.PD_AJUSTES_CNRP a
		where rel.id_mes = a.id_mes_anterior
			and rel.id_cliente = a.id_cliente
			and rel.id_condicion != a.id_condicion
			and a.id_mes = @idMes
			and a.nivel_ajuste = 'REL_CNRP_PAIS'

		set @msg = 'Updates Retrospectivo (PAIS): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

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
-- truncate table stg.PD_AJUSTES_CNRP 
-- Se debe correr ascendente ya q no todas las combinaciones estan cada mes (sin fact)
exec etl.map_adj_ajustes_cnrp 0, 202101
exec etl.map_adj_ajustes_cnrp 0, 202102
exec etl.map_adj_ajustes_cnrp 0, 202103
exec etl.map_adj_ajustes_cnrp 0, 202104
exec etl.map_adj_ajustes_cnrp 0, 202105
exec etl.map_adj_ajustes_cnrp 0, 202106
exec etl.map_adj_ajustes_cnrp 0, 202107
exec etl.map_adj_ajustes_cnrp 0, 202108
exec etl.map_adj_ajustes_cnrp 0, 202109
exec etl.map_adj_ajustes_cnrp 0, 202110
exec etl.map_adj_ajustes_cnrp 0, 202111
exec etl.map_adj_ajustes_cnrp 0, 202112

exec etl.map_adj_ajustes_cnrp 0, 202201
exec etl.map_adj_ajustes_cnrp 0, 202202
exec etl.map_adj_ajustes_cnrp 0, 202203
*/