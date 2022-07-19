use MX_DWH
Go

if object_id ('etl.map_rels_cnrp', 'P') is not null
	drop procedure etl.map_rels_cnrp
GO

create procedure etl.map_rels_cnrp (
	@idBatch numeric(15),
	@idMes numeric(10))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 17/05/2022
	-- Description:	Mapping Relaciones CNRP
	-- =============================================
	-- 13/06/2022 - Version Inicial

	set nocount on

	begin try
		declare @msg varchar(500), 
			@idMesRel numeric(10), @anio numeric(10), @nro_mes numeric(2)

		-- Inicio
		set @msg = 'Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- Mes a Procesar
		if @idMes > 0
			set @msg = 'Id Mes: ' + cast(@idMes as varchar)
		else
			set @msg = 'Id Mes: Todos (0)'

		exec etl.prc_Logging @idBatch, @msg

		create table #rel_ts (
			id_cliente numeric(10),
			id_sucursal numeric(10),
			id_tipo_serv_detalle numeric(10),
			id_mes numeric(10),
			cod_condicion varchar(30) COLLATE database_default,
			marca_clte_nuevo numeric(1) default 0,
			primary key (id_cliente, id_sucursal, id_tipo_serv_detalle, id_mes))

		create table #rel_suc (
			id_cliente numeric(10),
			id_sucursal numeric(10),
			id_mes numeric(10),
			cod_condicion varchar(30) COLLATE database_default,
			primary key (id_cliente, id_sucursal, id_mes))

		create table #rel_pais (
			id_cliente numeric(10),
			id_mes numeric(10),
			cod_condicion varchar(30) COLLATE database_default,
			primary key (id_cliente, id_mes))

		begin transaction
		
		-- Bajada
		select a.id_mes, a.id_cliente, a.id_sucursal, 
			a.id_condic_subclte as id_condicion,
			a.id_tipo_serv_detalle as id_tipo_serv_detalle,
			max(b.cod_condicion) as cod_condicion,
			cast ('-1' as varchar) as cod_condicion_new,
			sum(a.Importe_Fact ) as facturacion
		into #base
		from stg.PD_FT_FACTURACION_TS a,
			dbo.DCTE_LK_CONDICION b
		where a.id_condic_subclte = b.id_condicion
			and ((a.id_mes = @idMes and @idMes > 0) or (@idMes = 0))
			--and a.id_mes = 202201
		group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle, a.id_condic_subclte
		--having sum(a.Importe_Fact) != 0

		set @msg = 'Temporal (Base): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		create index #base_idx on #base (id_mes, id_cliente, id_sucursal, id_tipo_serv_detalle) 

		-- Agrega Perdidos
		insert into #base (id_mes,
			id_cliente, id_sucursal, id_condicion, id_tipo_serv_detalle, 
			cod_condicion, cod_condicion_new, facturacion)
		select id_mes,
			id_cliente, id_sucursal, max(id_condicion), id_tipo_serv_detalle, 
			max(cod_condicion), max(cod_condicion_new), sum(facturacion)
		from (
			select cast(ods.periodo as numeric) as id_mes, 
				isnull(c.id_cliente,-1) as id_cliente,
				etl.fn_LookupIdSucursal(ods.cod_sucursal) as id_sucursal,
				etl.fn_LookupIdCondicionCte( ods.cod_condicion_final ) as id_condicion,
				etl.fn_LookupIdTipoServDetalle(ods.cod_servicio) as id_tipo_serv_detalle,
				ods.cod_condicion_final as cod_condicion,
				cast ('-1' as varchar) as cod_condicion_new,
				isnull(ods.venta_actual,0) as facturacion
			from dbo.ODS_CNRP ods
				left outer join dbo.DCTE_LK_CLIENTE c 
				on c.cod_cliente = ods.cod_cliente
			where periodo = @idMes
				and ods.cod_condicion_final = 'P') ods
		where not exists (select 1 from #base b
				where b.id_mes = ods.id_mes
					and b.id_cliente = ods.id_cliente
					and b.id_sucursal = ods.id_sucursal
					and b.id_tipo_serv_detalle = ods.id_tipo_serv_detalle)
		group by id_mes, id_cliente, id_sucursal, id_tipo_serv_detalle

		set @msg = 'Agrega Perdidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-----------------------------
		-- Relacion Tipo Servicio (TS)
		----------------------------
		exec etl.prc_Logging @idBatch, '* Relacion Tipo Servicio (TS)'

		Insert into #rel_ts (id_mes, id_cliente, id_sucursal, id_tipo_serv_detalle, cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal, id_tipo_serv_detalle,
			max(cod_condicion) as cod_condicion
		from #base a
		where a.id_condicion > 0
		group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle
		having count(distinct cod_condicion) = 1 and max(cod_condicion) In ('N','P')


		set @msg = 'Nuevos o Perdidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
		
		Insert into #rel_ts (id_mes, id_cliente, id_sucursal, id_tipo_serv_detalle, cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal, id_tipo_serv_detalle,
			'N' as cod_condicion
		from #base a
		where a.id_condicion > 0
			and not exists (select 1 
					from #base b
					where b.id_cliente = a.id_cliente
						and b.id_sucursal = a.id_sucursal
						and b.id_tipo_serv_detalle = a.id_tipo_serv_detalle
						and b.id_mes = a.id_mes
						and b.cod_condicion = 'R')
		group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle
		having count(distinct cod_condicion) > 1 

		set @msg = 'Solo Nuevos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		Insert into #rel_ts (id_mes, id_cliente, id_sucursal, id_tipo_serv_detalle, cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal, id_tipo_serv_detalle,
			max(cod_condicion) as cod_condicion
		from #base a
		where a.id_condicion > 0
		group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle
		having count(distinct cod_condicion) = 1 and max(cod_condicion) In ('C')

		set @msg = 'Cedidos (1): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into #rel_ts (id_mes, id_cliente, id_sucursal, id_tipo_serv_detalle,
			cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
			'C' as cod_condicion
		from #base a
		where a.id_condicion > 0
			and cod_condicion = 'C'
			and not exists (select 1 
					from #rel_ts b
					where b.id_cliente = a.id_cliente
						and b.id_sucursal = a.id_sucursal
						and b.id_tipo_serv_detalle = a.id_tipo_serv_detalle
						and b.id_mes = a.id_mes)
		group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle

		set @msg = 'Cedidos (2): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into #rel_ts (id_mes, id_cliente, id_sucursal, id_tipo_serv_detalle,
			cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle,
			'R' as cod_condicion
		from #base a
		where a.id_condicion > 0
			and cod_condicion not in ('C', 'P')
			and not exists (select 1 
					from #rel_ts b
					where b.id_cliente = a.id_cliente
						and b.id_sucursal = a.id_sucursal
						and b.id_tipo_serv_detalle = a.id_tipo_serv_detalle
						and b.id_mes = a.id_mes)
		group by a.id_mes, a.id_cliente, a.id_sucursal, a.id_tipo_serv_detalle
		
		set @msg = 'Retenidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into #rel_ts (id_mes, id_cliente, id_sucursal, id_tipo_serv_detalle, 
			cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal, id_tipo_serv_detalle,
			'NA' as cod_condicion
		from #base a
		where a.id_condicion = 0
			and not exists (select 1 
					from #rel_ts b
					where b.id_cliente = a.id_cliente
						and b.id_sucursal = a.id_sucursal
						and b.id_tipo_serv_detalle = a.id_tipo_serv_detalle
						and b.id_mes = a.id_mes)
		group by a.id_mes, a.id_cliente, a.id_sucursal, id_tipo_serv_detalle
		
		set @msg = 'Condicion NA (--> R): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
		
		-- Ajustes Exceciones (TS)
		update a set a.cod_condicion = ods.cod_condicion_destino
		from #rel_ts a,
			dbo.DCTE_LK_CLIENTE b,
			dbo.DGEO_LK_SUCURSAL c,
			dbo.DFAC_LK_TIPO_SERVICIO_DETALLE d,
			(select * from dbo.ODS_EXCEPCION_RELS_CNRP 
				where año = (select anio from dbo.DTPO_LK_MESES where id_mes = (select max(id_mes) from #rel_ts))
					and TIPO_REL = 'REL_CNRP_TS') ods
		where a.id_mes in (select id_mes from #rel_ts)
			and a.id_cliente = b.id_cliente
			and a.id_sucursal = c.id_sucursal
			and a.id_tipo_serv_detalle = d.id_tipo_serv_detalle
			and b.cod_cliente = ods.cod_cliente
			and c.cod_sucursal = ods.cod_sucursal
			and d.cod_tipo_serv_detalle = ods.cod_tipo_serv_detalle
			and a.cod_condicion = ods.cod_condicion_origen

		set @msg = 'Excepciones: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
				
		-- Carga Cliente Nuevo
		set @msg = '- Set Marca Cliente Nuevo'; exec etl.prc_Logging @idBatch, @msg

		declare lc_cursor cursor for
		select distinct ms.id_mes, ms.nro_mes, ms.anio
		from #rel_ts rel, dbo.DTPO_LK_MESES ms
		where rel.id_mes = ms.id_mes
		order by 1

		open lc_cursor
		fetch next from lc_cursor into @idMesRel, @nro_mes, @anio
		while @@fetch_status = 0
			begin

				if @nro_mes = 1
						update REL
						set marca_clte_nuevo = 1
						from #rel_ts REL
						where REL.id_mes = @idMesRel
							and REL.cod_condicion = 'N'
							and exists (select 1
									from #base ft
									where ft.id_mes = REL.id_mes
										and ft.id_cliente = REL.id_cliente
										and ft.id_sucursal = REL.id_sucursal
										and ft.id_tipo_serv_detalle = REL.id_tipo_serv_detalle
										and ft.facturacion <> 0
										)
				else
					update REL
					set marca_clte_nuevo = 1
					from #rel_ts REL
					where REL.id_mes = @idMesRel
						and REL.cod_condicion = 'N'
						and not exists (
							select 1 
							from dbo.FT_FACTURACION_TS ft,
								(select * from dbo.DTPO_LK_MESES 
									where anio = @anio
										and nro_mes = 1) ms
							where ft.id_mes >= ms.id_mes
								and ft.id_mes < REL.id_mes
								and ft.id_cliente = REL.id_cliente
								and ft.id_sucursal = REL.id_sucursal
								and ft.id_tipo_serv_detalle = REL.id_tipo_serv_detalle
								and ft.Importe_Fact <> 0
								)

				
				set @msg = 'Mes: ' + convert(varchar, @idMesRel) + ' - Actualizados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

				-- next
				fetch next from lc_cursor into @idMesRel, @nro_mes, @anio
			end
		close lc_cursor
		deallocate lc_cursor

		-- Borrar Datos
		delete from REL from dbo.REL_CNRP_TS REL 
		where id_mes = @idMes or @idMes = 0

		set @msg = 'Borrados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Insertar Datos
		insert into dbo.REL_CNRP_TS (id_mes, id_cliente, 
			id_sucursal, id_tipo_serv_detalle, id_condicion, marca_clte_nuevo)
		select a.id_mes, a.id_cliente, 
			a.id_sucursal, a.id_tipo_serv_detalle, b.id_condicion, marca_clte_nuevo
		from #rel_ts a,
			dbo.DCTE_LK_CONDICION b
		where a.cod_condicion = b.cod_condicion
		order by a.id_mes, a.id_cliente

		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
		
		-----------------------------
		-- Relacion SUCURSAL
		----------------------------
		exec etl.prc_Logging @idBatch, '* Relacion Sucursal'

		Insert into #rel_suc (id_mes, id_cliente, id_sucursal, cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal, 
			max(cod_condicion) as cod_condicion
		from #rel_ts a
		where a.cod_condicion <> 'NA'
		group by a.id_mes, a.id_cliente, a.id_sucursal
		having count(distinct cod_condicion) = 1 and max(cod_condicion) In ('N','P')

		set @msg = 'Nuevos o Perdidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		Insert into #rel_suc (id_mes, id_cliente, id_sucursal, cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal,
			'N' as cod_condicion
		from #rel_ts a
		where a.cod_condicion != 'NA'
			and not exists (select 1 
					from #base b
					where b.id_cliente = a.id_cliente
						and b.id_sucursal = a.id_sucursal
						and b.id_mes = a.id_mes
						and b.cod_condicion = 'R')
		group by a.id_mes, a.id_cliente, a.id_sucursal
		having count(distinct cod_condicion) > 1 

		set @msg = 'Solo Nuevos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		Insert into #rel_suc (id_mes, id_cliente, id_sucursal, cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal, 
			max(cod_condicion) as cod_condicion
		from #rel_ts a
		where a.cod_condicion <> 'NA'
		group by a.id_mes, a.id_cliente, a.id_sucursal
		having count(distinct cod_condicion) = 1 and max(cod_condicion) In ('C')

		set @msg = 'Cedidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
				
		insert into #rel_suc (id_mes, id_cliente, id_sucursal, cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal, 
			'R' as cod_condicion
		from #rel_ts a
		where a.cod_condicion <> 'NA'
			and not exists (select 1 
					from #rel_suc b
					where b.id_cliente = a.id_cliente
						and b.id_sucursal = a.id_sucursal
						and b.id_mes = a.id_mes)
		group by a.id_mes, a.id_cliente, a.id_sucursal
		
		set @msg = 'Retenidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into #rel_suc (id_mes, id_cliente, id_sucursal, cod_condicion)
		select a.id_mes, a.id_cliente, a.id_sucursal, 
			'NA' as cod_condicion
		from #rel_ts a
		where a.cod_condicion = 'NA'
			and not exists (select 1 
					from #rel_suc b
					where b.id_cliente = a.id_cliente
						and b.id_sucursal = a.id_sucursal
						and b.id_mes = a.id_mes)
		group by a.id_mes, a.id_cliente, a.id_sucursal
		
		set @msg = 'Condicion NA: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg


		-- Ajustes Exceciones (SUC)
		update a set a.cod_condicion = ods.cod_condicion_destino
		from #rel_suc a,
			dbo.DCTE_LK_CLIENTE b,
			dbo.DGEO_LK_SUCURSAL c,
			(select * from dbo.ODS_EXCEPCION_RELS_CNRP 
				where año = (select anio from dbo.DTPO_LK_MESES where id_mes = (select max(id_mes) from #rel_suc))
					and TIPO_REL = 'REL_CNRP_SUCURSAL') ods
		where a.id_mes in (select id_mes from #rel_suc)
			and a.id_cliente = b.id_cliente
			and a.id_sucursal = c.id_sucursal
			and b.cod_cliente = ods.cod_cliente
			and c.cod_sucursal = ods.cod_sucursal
			and a.cod_condicion = ods.cod_condicion_origen

		set @msg = 'Excepciones: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg


		-- Borrar Datos
		delete from REL from dbo.REL_CNRP_SUCURSAL REL 
		where id_mes = @idMes or @idMes = 0

		set @msg = 'Borrados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Insertar Datos
		insert into dbo.REL_CNRP_SUCURSAL (id_mes, id_cliente, id_sucursal, id_condicion)
		select a.id_mes, a.id_cliente, 
			a.id_sucursal, b.id_condicion
		from #rel_suc a,
			dbo.DCTE_LK_CONDICION b
		where a.cod_condicion = b.cod_condicion
		order by a.id_mes, a.id_cliente

		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		---------------------
		-- Relacion PAIS
		--------------------
		exec etl.prc_Logging @idBatch, '* Relacion Pais'

		Insert into #rel_pais (id_mes, id_cliente, cod_condicion)
		select a.id_mes, a.id_cliente, 
			max(cod_condicion) as cod_condicion
		from #rel_suc a
		where a.cod_condicion <> 'NA'
		group by a.id_mes, a.id_cliente
		having count(distinct cod_condicion) = 1 and max(cod_condicion) In ('N','P')

		set @msg = 'Nuevos o Perdidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		Insert into #rel_pais (id_mes, id_cliente, cod_condicion)
		select a.id_mes, a.id_cliente, 
			'N' as cod_condicion
		from #rel_suc a
		where a.cod_condicion != 'NA'
			and not exists (select 1 
					from #base b
					where b.id_cliente = a.id_cliente
						and b.id_mes = a.id_mes
						and b.cod_condicion = 'R')
		group by a.id_mes, a.id_cliente, a.id_sucursal
		having count(distinct cod_condicion) > 1 

		set @msg = 'Solo Nuevos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		Insert into #rel_pais (id_mes, id_cliente, cod_condicion)
		select a.id_mes, a.id_cliente, 
			max(cod_condicion) as cod_condicion
		from #rel_suc a
		where a.cod_condicion <> 'NA'
		group by a.id_mes, a.id_cliente
		having count(distinct cod_condicion) = 1 and max(cod_condicion) In ('C')

		set @msg = 'Cedidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
		
		insert into #rel_pais (id_mes, id_cliente, cod_condicion)
		select a.id_mes, a.id_cliente, 
			'R' as cod_condicion
		from #rel_suc a
		where a.cod_condicion <> 'NA'
			and not exists (select 1 
						from #rel_pais b
						where b.id_mes = a.id_mes
							and b.id_cliente = a.id_cliente)
		group by a.id_mes, a.id_cliente
		
		set @msg = 'Retenidos: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		insert into #rel_pais (id_mes, id_cliente, cod_condicion)
		select a.id_mes, a.id_cliente, 
			'NA' as cod_condicion
		from #rel_suc a
		where a.cod_condicion = 'NA'
			and not exists (select 1 
						from #rel_pais b
						where b.id_mes = a.id_mes
							and b.id_cliente = a.id_cliente)
		group by a.id_mes, a.id_cliente

		set @msg = 'Condicion NA: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Borrar Datos 
		delete from REL from dbo.REL_CNRP_PAIS REL
		where id_mes = @idMes or @idMes = 0

		set @msg = 'Borrados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Insertar Datos
		insert into dbo.REL_CNRP_PAIS (id_mes, id_cliente, id_condicion)
		select a.id_mes, a.id_cliente, b.id_condicion
		from #rel_pais a,
			dbo.DCTE_LK_CONDICION b
		where a.cod_condicion = b.cod_condicion
		order by a.id_mes, a.id_cliente

		set @msg = 'Insertados: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Ajustes NA --> R
		update rel
		set id_condicion = etl.fn_LookupIdCondicionCte ('R')
		from dbo.REL_CNRP_TS rel
		where id_mes in (select distinct id_mes from #base)
			and id_condicion = etl.fn_LookupIdCondicionCte ('NA')

		set @msg = 'Ajustes NA --> R (TS): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		update rel
		set id_condicion = etl.fn_LookupIdCondicionCte ('R')
		from dbo.REL_CNRP_SUCURSAL rel
		where id_mes in (select distinct id_mes from #base)
			and id_condicion = etl.fn_LookupIdCondicionCte ('NA')

		set @msg = 'Ajustes NA --> R (SUC): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		update rel
		set id_condicion = etl.fn_LookupIdCondicionCte ('R')
		from dbo.REL_CNRP_PAIS rel
		where id_mes in (select distinct id_mes from #base)
			and id_condicion = etl.fn_LookupIdCondicionCte ('NA')

		set @msg = 'Ajustes NA --> R (PAIS): ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

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

exec etl.map_rels_cnrp 0, 202101
exec etl.map_rels_cnrp 0, 202102
exec etl.map_rels_cnrp 0, 202103
exec etl.map_rels_cnrp 0, 202104
exec etl.map_rels_cnrp 0, 202105
exec etl.map_rels_cnrp 0, 202106
exec etl.map_rels_cnrp 0, 202107
exec etl.map_rels_cnrp 0, 202108
exec etl.map_rels_cnrp 0, 202109
exec etl.map_rels_cnrp 0, 202110
exec etl.map_rels_cnrp 0, 202111
exec etl.map_rels_cnrp 0, 202112

exec etl.map_rels_cnrp 0, 202201
exec etl.map_rels_cnrp 0, 202202
exec etl.map_rels_cnrp 0, 202203

*/