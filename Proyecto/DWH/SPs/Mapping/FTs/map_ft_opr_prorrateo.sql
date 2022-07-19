use MX_DWH
Go

if object_id ('etl.map_ft_opr_prorrateo', 'P') is not null
	drop procedure etl.map_ft_opr_prorrateo
GO

create procedure etl.map_ft_opr_prorrateo (
	@idBatch numeric(15),
	@idMes numeric(10))
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 26/05/2022
	-- Description:	Mapping FT_OPR - Prorrateo
	-- =============================================
	-- 14/07/2022 - Version Inicial
	
	set nocount on

	begin try

		declare @msg varchar(500), @msgLog varchar(500), @cant numeric(10)
	

		-- Inicio
		set @msg = '- Mapping: ' + Upper( object_name(@@procid)); exec etl.prc_Logging @idBatch, @msg

		-- Mes/es
		set @msg = 'Id Mes: ' + convert(varchar, @idMes); exec etl.prc_Logging @idBatch, @msg

		-- Verificar si Existen Ajuste de Reclasificacion
		if not exists( select max(1) from dbo.FT_OPR_DETALLE_CUENTA ft
					where id_mes = @idMes	
						and (origen in ('Reclas FRT Package', 'Reclas Virtual IF')
							or origen like '%reclas%') )
			begin
				set @msg = 'No existe Ajustes de Reclasificacion en el Periodo. No se continua con el Prorrateo'
				set @msgLog = '<b>** WARNING: '+@msg+'.</b>' ; 
				exec etl.prc_Logging @idBatch, @msgLog
				return
			end

		-- drop table #allocations
		-- Allocations
		select orden_present as orden, 
			ods.id_mes, 
			ods.id_cc, 
			ods.cod_cc,
			ods.id_agrupador,
			ods.cod_agrupador,
			ods.id_zona,
			ods.cod_zona,
			ods.desc_zona, 
			ods.Allocation,
			case when ods.dir_prorrateo = 'SI' then 1 else 0 end as CC_Direc
		into #allocations
		-- select *
		from dbo.VW_LK_CC_OPR_HIST ods
		where ods.id_zona > 0
			and orden_present > 0
			and ods.Allocation is not null
			and id_mes = @idMes
		order by orden_present

		set @cant = @@rowcount
		set @msg = 'Allocations: ' + convert(varchar, @cant); exec etl.prc_Logging @idBatch, @msg	
		
		if @cant <= 0
			begin
				set @msg = 'No se encontraron las Allocations configuradas en el Periodo. No se continua con el Prorrateo'
				set @msgLog = '<b>** WARNING: '+@msg+'.</b>' ; 
				exec etl.prc_Logging @idBatch, @msgLog
				return
			end

		-- Base Zona
		select vw.id_mes, vw.id_empresa, vw.cod_empresa, 
			vw.id_cc, vw.cod_cc, vw.CC_Direc,
			vw.id_agrupador, vw.cod_agrupador, 
			vw.id_zona, vw.Desc_Zona, vw.cod_zona, vw.Allocation, vw.Dir_allocation,
			sum(venta) as Venta,
			sum(venta+costo) as MB,
			sum(gasto) as Gasto
		into #base_zona
		from (
			select ft.id_mes, ft.id_empresa, em.cod_empresa, 
				ods.id_cc,
				ods.cod_cc, 
				ods.id_agrupador,
				ods.cod_agrupador,
				ods.CC_Direc,
				case when left(ln.cod_cuenta,2) = '60' then real else 0 end as venta,
				case when left(ln.cod_cuenta,2) = '65' then real else 0 end as costo,
				case when left(ln.cod_cuenta,2) in ('70', '72', '73', '74', '75', '76', '79') then real else 0 end as gasto,
				al.id_zona,
				al.cod_zona,
				al.Desc_Zona,
				al.Allocation,
				al.cod_cc as dir_allocation
			from dbo.FT_OPR_DETALLE_CUENTA ft,
				dbo.DEMP_LK_EMPRESA em,
				dbo.DESG_LK_LINEA ln,
				#allocations al,
				(select a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.desc_zona,
					max(case when b.dir_prorrateo = 'SI' then 1 else 0 end) as CC_Direc
				-- select *
				from dbo.VW_LK_CC_OPR_HIST a
					left outer join dbo.VW_LK_CC_OPR_HIST b
						on a.id_mes = b.id_mes
							and a.cod_agrupador = b.cod_cc
				group by a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.desc_zona) ods
			where ft.id_mes = ods.id_mes
				and ft.id_cc_orig = ods.id_cc
				and ft.id_empresa = em.id_empresa
				and ft.id_linea = ln.id_linea
				and ft.origen not in ('TOTAL', 'PRORRATEO', 'DSO')
				and left(ln.cod_cuenta,2) in ('60','65','70', '72', '73', '74', '75', '76', '79')
				and ods.id_mes = al.id_mes
				and ods.id_zona = al.id_zona
				and al.Allocation = 'Gastos Zona'
			) vw
		group by vw.id_mes, vw.id_empresa, vw.id_cc, vw.id_agrupador, vw.id_zona,
			vw.cod_empresa, 
			vw.cod_cc, 
			vw.cod_agrupador,
			vw.CC_Direc,
			vw.cod_zona,
			vw.Desc_Zona,
			vw.Allocation, 
			vw.dir_allocation
		order by 1,2, vw.Desc_Zona, vw.CC_Direc, vw.cod_agrupador

		set @msg = 'Base Zona: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Subtotales x Zona
		-- drop table #totales_zona
		select p.*, ln.id_linea, ln.desc_linea, 
			isnull(cc.id_cuenta,0) as id_cuenta, isnull(sr.id_tipo_serv_detalle,0) as id_tipo_serv_detalle
		into #totales_zona
		from (
			select id_mes, id_empresa, cod_empresa, cod_zona, id_zona, Desc_Zona, Allocation, Dir_Allocation,
				sum(Venta) as Venta,
				sum(MB) as MB,
				sum(case when CC_Direc = 1 then Gasto else 0 end) as Gasto_Zona
			from #base_zona
			group by id_mes, id_empresa, cod_empresa, cod_zona, id_zona, Desc_Zona, Allocation, Dir_Allocation) p
			left outer join DESG_LK_LINEA ln
				on ln.DESC_LINEA = 'Allocation ' + Allocation collate database_default
					left outer join dbo.DGEN_LK_CTA_CONTABLE cc
						on cc.cod_cuenta = ln.cod_cuenta
					left outer join dbo.DFAC_LK_TIPO_SERVICIO_DETALLE sr
						on sr.cod_tipo_serv_detalle = ln.cod_servicio

		set @msg = 'Totales Zona: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg		

		create unique index #totales_zona_uk on #totales_zona (id_mes, id_empresa, id_zona)

		-- Prorrateo Zona
		-- drop table  #prorrateo_zona
		select b.id_mes, 'PRORRATEO' as origen,
			b.id_cc, 
			0 as id_cliente, 
			0 as id_subclte,
			b.id_empresa,
			t.id_linea,
			t.id_cuenta,
			t.id_tipo_serv_detalle,
			round(
				case when CC_Direc = 1 then
					b.Gasto * -1
				else
					(((case when t.Venta = 0 then 0 else cast( cast(b.Venta  as float)/ cast(t.Venta as float) as numeric(38,12)) end) * 0.7) * t.Gasto_Zona) +
					(((case when t.MB = 0 then 0 else cast( cast(b.MB as float)/ cast(t.MB as float) as numeric(38,12)) end) * 0.3) * t.Gasto_Zona) 
				end, 4) as real,
			0 as budget, 
			0 as real_num, 
			0 as real_den,
			b.cod_empresa,
			b.id_zona,
			b.cod_zona,
			b.Desc_Zona,
			b.Dir_allocation,
			b.cod_cc,
			b.id_agrupador,
			b.cod_agrupador,
			b.Venta,
			b.MB,
			b.Gasto
		into #prorrateo_zona
		from #base_zona b,
			#totales_zona t
		where b.id_mes = t.id_mes
			and b.id_empresa = t.id_empresa
			and b.id_zona = t.id_zona

		set @msg = 'Prorrateo Zona: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg	

		/*
		
		select cod_agrupador, replace(sum(real),'.',',') allocation
		from #prorrateo_zona
		where desc_zona = 'Norte'
		group by cod_agrupador
		order by 1

		select cod_empresa, desc_zona, replace(sum(real),'.',',') allocation
		from #prorrateo_zona
		--where desc_zona = 'Norte'
		group by cod_empresa, desc_zona
		order by 1
		*/

		-- Gasto Global
		select vw.id_mes, vw.id_zona, vw.cod_zona, vw.desc_zona, vw.Allocation,
			sum(gasto) as Gasto_Total_Global,
			max(vw.cod_agrupador) as cod_agrupador,
			max(vw.id_agrupador) as id_agrupador
		into #gasto_global
		from (
			select vw.id_mes, 
				vw.id_zona,
				vw.Allocation,
				vw.cod_zona,
				vw.desc_zona,
				case when vw.CC_Direc = 1 then vw.id_agrupador else 0 end as id_agrupador, 
				case when vw.CC_Direc = 1 then vw.cod_agrupador else '0' end as cod_agrupador, 
				Gasto
			from (
				select ft.id_mes, ft.id_empresa, em.cod_empresa, 
					ods.id_cc,
					ods.cod_cc, 
					ods.id_agrupador,
					ods.cod_agrupador,
					ods.CC_Direc,
					case when left(ln.cod_cuenta,2) in ('70', '72', '73', '74', '75', '76', '79') then real else 0 end as gasto,
					al.id_zona,
					al.cod_zona,
					al.Desc_Zona,
					al.Allocation,
					al.cod_cc as dir_allocation
				from dbo.FT_OPR_DETALLE_CUENTA ft,
					dbo.DEMP_LK_EMPRESA em,
					dbo.DESG_LK_LINEA ln,
					#allocations al,
					(select a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona,
						max(case when b.dir_prorrateo = 'SI' then 1 else 0 end) as CC_Direc
					-- select *
					from dbo.VW_LK_CC_OPR_HIST a
						left outer join dbo.VW_LK_CC_OPR_HIST b
							on a.id_mes = b.id_mes
								and a.cod_agrupador = b.cod_cc
					group by a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona) ods
				where ft.id_mes = ods.id_mes
					and ft.id_cc_orig = ods.id_cc
					and ft.id_empresa = em.id_empresa
					and ft.id_linea = ln.id_linea
					and ft.origen not in ('TOTAL', 'PRORRATEO', 'DSO')
					and left(ln.cod_cuenta,2) in ('70', '72', '73', '74', '75', '76', '79')
					and ods.id_mes = al.id_mes
					and ((ods.cod_agrupador = al.cod_cc and al.Allocation != 'Gastos HQ')
						Or (al.Allocation = 'Gastos HQ' and ods.desc_zona = al.desc_zona))
					and al.Allocation != 'Gastos Zona'
				) vw 
			) vw
		group by vw.id_mes, vw.id_zona, vw.cod_zona, vw.desc_zona, vw.Allocation
		order by 1,2,3,4

		set @msg = 'Gasto Global: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Base Venta y MB
		-- drop table #base_VentaMB
		select vw.id_mes, vw.id_empresa, vw.id_cc, vw.id_agrupador, vw.id_zona,
			vw.cod_empresa, vw.cod_cc, vw.cod_agrupador, vw.cod_zona, vw.desc_zona,
			vw.Allocation1, vw.Allocation2, vw.Allocation3, vw.Allocation4,
			vw.LineaServicio, vw.ServicioGrupo,
			sum(venta) as Venta,
			sum(venta+costo) as MB
		into #base_VentaMB
		from (
			select ft.id_mes, ft.id_empresa, ft.id_cc_opr as id_cc,
				ods.id_agrupador, ods.id_zona, 
				em.cod_empresa, ods.cod_cc, 
				ods.cod_agrupador, ods.cod_zona, ods.desc_zona,
				ods.CC_Direc,
				case when left(ln.cod_cuenta,2) = '60' then real else 0 end as venta,
				case when left(ln.cod_cuenta,2) = '65' then real else 0 end as costo,
				se.cod_tipo_serv_detalle as LineaServicio,
				gs.desc_tipo_servicio2 as ServicioGrupo,
				case when se.cod_tipo_serv_detalle in ('MX_TRAINING', 'MX_CONSULTING') then
						'Training & Consulting'
					when  gs.desc_tipo_servicio2 = 'Permanent' then
						'Permanent'
					else
						'Resto'
				end as Allocation1,
				case when em.cod_empresa in ('1181', '1185', '2012', '2015', '2018') then
						'Gastos Op. Industrial'
					else
						'Resto'
				end as Allocation2,
				case when ods.cod_zona not in ('SP', 'TO', 'WT')  then
						'Operaciones'
					else
						'Resto'
				end as Allocation3,
				case when ods.cod_zona not in ('SP') then
						'Gastos HQ'
					else
						'Resto'
				end as Allocation4
			from dbo.FT_OPR_DETALLE_CUENTA ft,
				dbo.DEMP_LK_EMPRESA em,
				dbo.DESG_LK_LINEA ln,
				dbo.DGEN_LK_CTA_CONTABLE cu,
				dbo.DFAC_LK_TIPO_SERVICIO_DETALLE se, 
				dbo.DFAC_LK_SERVICIO2 gs,
				(select a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona,
					max(case when b.dir_prorrateo = 'SI' then 1 else 0 end) as CC_Direc
				from dbo.VW_LK_CC_OPR_HIST a
					left outer join dbo.VW_LK_CC_OPR_HIST b
						on a.id_mes = b.id_mes
							and a.cod_agrupador = b.cod_cc
				group by a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona) ods
			where 1=1
				and ft.id_empresa = em.id_empresa
				and ft.id_linea = ln.id_linea
				and ft.id_cuenta = cu.id_cuenta
				and ft.id_mes = ods.id_mes
				and ft.id_cc_orig = ods.id_cc
				and ft.id_tipo_serv_detalle = se.id_tipo_serv_detalle
				and se.ID_TIPO_SERVICIO2 = gs.ID_TIPO_SERVICIO2
				and ft.id_mes = @idMes
				and origen not in ('TOTAL', 'PRORRATEO', 'DSO')
				and left(ln.cod_cuenta,2) in ('60','65')
				--and em.cod_empresa = '1181'
				--and ods.cod_agrupador = '910550'
				--and ods.desc_zona = 'Operaciones'
			) vw
		group by vw.id_mes, vw.id_empresa, vw.id_cc, vw.id_agrupador, vw.id_zona,
			vw.cod_empresa, vw.cod_cc, vw.cod_agrupador, vw.cod_zona, vw.desc_zona,
			vw.Allocation1, vw.Allocation2, vw.Allocation3, vw.Allocation4,
			vw.LineaServicio, vw.ServicioGrupo
			--vw.CC_Direc
		order by 1,2,5,4

		set @msg = 'Base Venta y MB: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg
		
		-- Venta Global
		-- drop table #VentaMB_Global
		select vw.*
		into #VentaMB_Global
		from (
			select Allocation1 as Allocation, sum(venta) as Venta_Global, sum(mb) as MB_Global
			from #base_VentaMB
			where Allocation1 in ('Training & Consulting', 'Permanent')
			group by Allocation1 
			union all
			select Allocation2, sum(venta) , sum(mb)
			from #base_VentaMB
			where Allocation2 in ('Gastos Op. Industrial')
			group by Allocation2 
			union all
			select Allocation3, sum(venta) , sum(mb)
			from #base_VentaMB
			where Allocation3 in ('Operaciones')
			group by Allocation3 
			union all
			select Allocation4, sum(venta) , sum(mb)
			from #base_VentaMB
			where Allocation4 in ('Gastos HQ')
			group by Allocation4 ) vw

		set @msg = 'Venta Global: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Venta Empresa
		-- drop table #VentaMB_Empresa
		select vw.*
		into #VentaMB_Empresa
		from (
			select cod_empresa, id_empresa, Allocation1 as Allocation, sum(venta) as Venta_Empresa , sum(mb) as MB_Empresa
			-- select *
			from #base_VentaMB
			where Allocation1 in ('Training & Consulting', 'Permanent')
			group by cod_empresa, id_empresa, Allocation1 
			union all
			select cod_empresa, id_empresa, Allocation2, sum(venta) , sum(mb)
			from #base_VentaMB
			where Allocation2 in ('Gastos Op. Industrial')
			group by cod_empresa, id_empresa, Allocation2 
			union all
			select cod_empresa, id_empresa, Allocation3, sum(venta) , sum(mb)
			from #base_VentaMB
			where Allocation3 in ('Operaciones')
			group by cod_empresa, id_empresa, Allocation3 
			union all
			select cod_empresa, id_empresa, 'Gastos HQ' as Allocation4, sum(venta) , sum(mb)
			-- select distinct cod_zona
			from #base_VentaMB
			where 1=1
				and cod_zona not in ('SP')
			group by cod_empresa, id_empresa) vw

		set @msg = 'Venta Empresa: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- select * from #gasto_Global
		-- select * from #VentaMB_Global
		-- select * from #VentaMB_Empresa
		-- select * from #base_VentaMB where Allocation3 = 'Operaciones'
		-- select * from #allocations where Allocation != 'Gastos Zona'

		-- Prorrateo Global
		-- drop table #Prorrateo_Global
		select p.*, 0 as Dir_allocation
		into #Prorrateo_Global
		from (
			select x.*,
				round(cast(
					(((case when x.Venta_Empresa = 0 then 0 else cast( cast(x.Venta as float) / cast(x.Venta_Empresa as float) as numeric(38,12)) end) * 0.7) * x.Gasto_Empresa) +
					(((case when x.MB_Empresa = 0 then 0 else cast( cast(x.MB as float) / cast(x.MB_Empresa as float) as numeric(38,12)) end) * 0.3) * x.Gasto_Empresa) 
					as numeric(25,5))
				,4)
				as Prorrateo
			from (
				select vw.*,
					ve.Venta_Empresa, ve.MB_Empresa, 
					vg.Venta_Global, vg.MB_Global, gb.Gasto_Total_Global as Gasto_Global, 
					(((case when vg.Venta_Global = 0 then 0 else cast( cast(ve.Venta_Empresa as float) / cast(vg.Venta_Global as float) as numeric(38,12)) end) * 0.7) * gb.Gasto_Total_Global) +
					(((case when vg.MB_Global = 0 then 0 else cast( cast(ve.MB_Empresa as float) / cast(vg.MB_Global as float) as numeric(38,12)) end) * 0.3) * gb.Gasto_Total_Global) as Gasto_Empresa
				from (
					-- Permanent y Training
					select id_mes, id_empresa, id_cc, id_agrupador, id_zona,
						cod_empresa, cod_cc, cod_agrupador, cod_zona, desc_zona,	
						Allocation1	as Allocation,
						sum(Venta) as Venta,
						sum(MB) as MB
					from #base_VentaMB bs
					where 1=1
						and bs.Allocation1 in ('Permanent', 'Training & Consulting')
					group by id_mes, id_empresa, id_cc, id_agrupador, id_zona,
						cod_empresa, cod_cc, cod_agrupador, cod_zona, desc_zona,	
						Allocation1
					union all
					-- Gastos Op. Industrial
					select id_mes, id_empresa, id_cc, id_agrupador, id_zona,
						cod_empresa, cod_cc, cod_agrupador, cod_zona, desc_zona,	
						Allocation2	as Allocation,
						sum(Venta) as Venta,
						sum(MB) as MB
					from #base_VentaMB bs
					where 1=1
						and bs.Allocation2 in ('Gastos Op. Industrial')
					group by id_mes, id_empresa, id_cc, id_agrupador, id_zona,
						cod_empresa, cod_cc, cod_agrupador, cod_zona, desc_zona,	
						Allocation2
					union all
					-- Operaciones
					select id_mes, id_empresa, id_cc, id_agrupador, id_zona,
						cod_empresa, cod_cc, cod_agrupador, cod_zona, desc_zona,	
						Allocation3	as Allocation,
						sum(Venta) as Venta,
						sum(MB) as MB
					from #base_VentaMB bs
					where 1=1
						and bs.Allocation3 in ('Operaciones')
					group by id_mes, id_empresa, id_cc, id_agrupador, id_zona,
						cod_empresa, cod_cc, cod_agrupador, cod_zona, desc_zona,	
						Allocation3
					union all
					-- Gastos HQ
					select id_mes, id_empresa, id_cc, id_agrupador, id_zona,
						cod_empresa, cod_cc, cod_agrupador, cod_zona, desc_zona,	
						'Gastos HQ' as Allocation4,
						sum(Venta) as Venta,
						sum(MB) as MB
					from #base_VentaMB bs
					where 1=1
						and bs.cod_zona not in ('SP')
					group by id_mes, id_empresa, id_cc, id_agrupador, id_zona,
						cod_empresa, cod_cc, cod_agrupador, cod_zona, desc_zona
					) vw,
					#VentaMB_Empresa ve,
					#VentaMB_Global vg,
					#gasto_Global gb
				where vw.id_empresa = ve.id_empresa
					and vw.Allocation = ve.Allocation
					and vw.Allocation = vg.Allocation
					and vw.id_mes = gb.id_mes
					and vw.Allocation = gb.Allocation 
					-- and vw.Allocation = 'Operaciones' and vw.cod_empresa = '1181'
					-- and vw.Allocation = 'Training & Consulting' and vw.cod_empresa = '1188'
					) x ) p
		order by cod_agrupador

		-- Resta Agrupadores de Direccion (menos SEDE)
		insert into #Prorrateo_Global (
			id_mes, id_empresa, id_cc, id_agrupador, id_zona, cod_empresa, cod_cc, 
			cod_agrupador, cod_zona, desc_zona, Allocation, Venta, MB, Venta_Empresa, 
			MB_Empresa, Venta_Global, MB_Global, Gasto_Global, Gasto_Empresa, 
			Prorrateo, Dir_allocation)
		select p.id_mes, p.id_empresa, al.id_cc, al.id_agrupador, al.id_zona, p.cod_empresa,
			al.cod_cc, al.cod_agrupador, al.cod_zona, al. desc_zona, p.Allocation,
			0 as Venta,	0 as MB, 0 as Venta_Empresa, 0 as MB_Empresa,	
			p.Venta_Global, p.MB_Global, p.Gasto_Global, p.Gasto_Empresa, Prorrateo,
			1 as Dir_allocation
		-- select p.*, al.*
		from (select id_mes, id_empresa, cod_empresa,
				Allocation, 
				max(Venta_Global) as Venta_Global,
				max(MB_Global) as MB_Global,
				max(Gasto_Global) as Gasto_Global,
				max(Gasto_Empresa) as Gasto_Empresa,
				max(Gasto_Empresa)*-1 as Prorrateo --- ??
			-- select *
			from #Prorrateo_Global
			group by id_mes, id_empresa, cod_empresa,
				Allocation) p,
			#allocations al
		where p.id_mes = al.id_mes
			and p.Allocation = al.Allocation
			and al.CC_Direc = 1
			and al.cod_agrupador not in ('910550','910100') -- Sede de Calcula Abajo
			and not exists (select 1 from #Prorrateo_Global pg
					where pg.id_mes = p.id_mes
						and pg.id_empresa = p.id_empresa
						and pg.id_agrupador = al.id_agrupador) -- Casos con Venta en mismo CC PResentacion

		-- Caso SEDE (Resta Gastos)
		insert into #Prorrateo_Global (
			id_mes, id_empresa, id_cc, id_agrupador, id_zona, cod_empresa, cod_cc, 
			cod_agrupador, cod_zona, desc_zona, Allocation, 
			Venta, MB, Venta_Empresa,  MB_Empresa, 
			Venta_Global, MB_Global, Gasto_Global, Gasto_Empresa, 
			Prorrateo, Dir_allocation)
		select vw.id_mes, vw.id_empresa, vw.id_cc, vw.id_agrupador, vw.id_zona, vw.cod_empresa, vw.cod_cc,
			vw.cod_agrupador, vw.cod_zona, max(desc_zona), Allocation,
			0 as Venta,	0 as MB, 0 as Venta_Empresa, 0 as MB_Empresa,	
			0 as Venta_Global, 0 as MB_Global, 0 as Gasto_Global, 
			max(Gst_HQ_Prorrateado) as Gasto_Empresa, 
			(sum(Gasto)*-1) + (case when max(vw.CC_Direc) = 1 then max(Gst_HQ_Prorrateado) else 0 end) as Prorrateo,
			max(vw.CC_Direc) as Dir_allocation
		from (
			select vw.id_mes, 
				vw.id_empresa, vw.cod_empresa,
				vw.id_cc, vw.cod_cc,
				vw.Allocation,
				vw.id_zona, vw.cod_zona, vw.desc_zona,
				vw.id_agrupador, 
				vw.cod_agrupador, 
				vw.Gasto,
				vw.Gst_HQ_Prorrateado,
				vw.CC_Direc
			-- select *
			from (
				select ft.id_mes, ft.id_empresa, em.cod_empresa, 
					ods.id_cc, ods.cod_cc, 
					ods.id_agrupador,
					ods.cod_agrupador,
					ods.CC_Direc,
					real as Gasto,
					al.id_zona,
					al.cod_zona,
					al.Desc_Zona,
					al.Allocation,
					al.cod_cc as dir_allocation,
					isnull(gts.Gasto_HQ_Prorrateado,0) as Gst_HQ_Prorrateado
				-- select sum(real)
				from dbo.FT_OPR_DETALLE_CUENTA ft,
					dbo.DEMP_LK_EMPRESA em,
					dbo.DESG_LK_LINEA ln,
					#allocations al,
					(select a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona,
						max(case when b.dir_prorrateo = 'SI' then 1 else 0 end) as CC_Direc
					-- select *
					from dbo.VW_LK_CC_OPR_HIST a
						left outer join dbo.VW_LK_CC_OPR_HIST b
							on a.id_mes = b.id_mes
								and a.cod_agrupador = b.cod_cc
					group by a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona) ods,
					(select id_mes, id_empresa, cod_empresa,
						Allocation,
						max(Gasto_Empresa) as Gasto_HQ_Prorrateado
					-- select *
					from #Prorrateo_Global
					where Allocation = 'Gastos HQ'
					group by id_mes, id_empresa, cod_empresa,
						Allocation) gts
				where ft.id_mes = ods.id_mes
					and ft.id_cc_orig = ods.id_cc
					and ft.id_empresa = em.id_empresa
					and ft.id_linea = ln.id_linea
					and ft.origen not in ('TOTAL', 'PRORRATEO', 'DSO')
					and left(ln.cod_cuenta,2) in ('70', '72', '73', '74', '75', '76', '79')
					and al.Allocation = gts.Allocation
					and al.id_mes = gts.id_mes
					and gts.id_empresa = em.id_empresa
					and ods.id_mes = al.id_mes
					and ods.cod_zona = al.cod_zona
				) vw 
			) vw
		where vw.cod_agrupador not in ('910550','910100') 
		group by vw.id_mes, vw.id_empresa, vw.id_cc, vw.id_agrupador, vw.id_zona, vw.cod_empresa, vw.cod_cc,
			vw.cod_agrupador, vw.cod_zona, Allocation
		order by 1,2,3,4

		-- Resta Agrupadores de Direccion  (Caso SEDE 910550 y 910100)
		-- venta - costo + gastos - el resultado del prorrateo
		-- drop table #Info_Dir_Grp_Sede
		select vw.id_mes, vw.id_empresa, vw.cod_empresa, vw.id_cc,
			vw.cod_cc, vw.id_agrupador, vw.cod_agrupador, max(CC_Direc) as CC_Direc,
			Sum(Venta) as Venta,
			Sum(Costo) as Costo,
			Sum(Gasto) as Gasto
		into #Info_Dir_Grp_Sede
		from (
			select ft.id_mes, ft.id_empresa, em.cod_empresa, 
				ods.id_cc, ods.cod_cc, 
				ods.id_agrupador,
				ods.cod_agrupador,
				ods.CC_Direc,
				case when left(ln.cod_cuenta,2) = '60' then real else 0 end as Venta,
				case when left(ln.cod_cuenta,2) = '65' then real else 0 end as Costo,
				case when left(ln.cod_cuenta,2) not in ('60','65') then real else 0 end as Gasto
				from dbo.FT_OPR_DETALLE_CUENTA ft,
					dbo.DEMP_LK_EMPRESA em,
					dbo.DESG_LK_LINEA ln,
					(select a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona,
						max(case when b.dir_prorrateo = 'SI' then 1 else 0 end) as CC_Direc
					-- select *
					from dbo.VW_LK_CC_OPR_HIST a
						left outer join dbo.VW_LK_CC_OPR_HIST b
							on a.id_mes = b.id_mes
								and a.cod_agrupador = b.cod_cc
					group by a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona) ods
				where ft.id_mes = ods.id_mes
					and ft.id_cc_orig = ods.id_cc
					and ft.id_empresa = em.id_empresa
					and ft.id_linea = ln.id_linea
					and ft.origen not in ('TOTAL', 'PRORRATEO', 'DSO')
					and left(ln.cod_cuenta,2) in ('60', '65', '70', '72', '73', '74', '75', '76', '79')
					and ft.id_mes = ods.id_mes
					and ft.id_cc_opr = ods.id_cc
					and ods.cod_agrupador in ('910550','910100')
					and ft.id_mes = @idMes
					) vw
		group by vw.id_mes, vw.id_empresa, vw.cod_empresa, vw.id_cc,
			vw.cod_cc, vw.id_agrupador, vw.cod_agrupador

		-- select * from #Info_Dir_Grp_Sede

		insert into #Prorrateo_Global (
			id_mes, id_empresa, id_cc, id_agrupador, id_zona, cod_empresa, cod_cc, 
			cod_agrupador, cod_zona, desc_zona, Allocation, 
			Venta, MB, Venta_Empresa,  MB_Empresa, 
			Venta_Global, MB_Global, Gasto_Global, Gasto_Empresa, 
			Prorrateo, Dir_allocation)
		select vw.id_mes, vw.id_empresa, vw.id_cc, vw.id_agrupador, vw.id_zona, vw.cod_empresa, vw.cod_cc, 
			vw.cod_agrupador, vw.cod_zona, vw.desc_zona, vw.Allocation, 
			vw.Venta, vw.MB, vw.Venta_Empresa, vw.MB_Empresa, 
			vw.Venta_Global, vw.MB_Global, vw.Gasto_Global, vw.Gasto_Empresa, 
			case vw.cod_agrupador 
				when '910550' then
					/*case when vw.cod_empresa != '1188' then
						isnull(inf.venta,0) - isnull(inf.costo,0) + isnull(inf.gasto,0) + vw.Prorrateo  -- (Vta - Cos + Gts - Prorr HQ)
					else
						vw.Prorrateo
					end*/
					vw.Prorrateo
				when '910100' then
					isnull(inf.gasto,0)*-1 + isnull(vw.Gasto_Empresa,0)
				else 
					vw.Prorrateo 
			end as Prorrateo, 
			vw.Dir_allocation
		from (
			select gts.id_mes, gts.id_empresa, ods.id_cc, ods.id_agrupador, ods.id_zona, gts.cod_empresa, ods.cod_cc,
				ods.cod_agrupador, ods.cod_zona, ods.desc_zona, gts.Allocation,
				0 as Venta,	0 as MB, 0 as Venta_Empresa, 0 as MB_Empresa,	
				0 as Venta_Global, 0 as MB_Global, 0 as Gasto_Global, 
				isnull(Gasto_HQ_Prorrateado,0) as Gasto_Empresa, 
				(isnull(Gasto_HQ_Prorrateado,0)*-1) as Prorrateo,
				ods.CC_Direc as Dir_allocation
			from (select a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona,
					0 as CC_Direc, 'Gastos HQ' as Allocation
				from dbo.VW_LK_CC_OPR_HIST a
				where cod_agrupador in ('910550','910100')) ods,
				(select id_mes, id_empresa, cod_empresa,
					Allocation,
					max(Gasto_Empresa) as Gasto_HQ_Prorrateado
				-- select *
				from #Prorrateo_Global
				where Allocation = 'Gastos HQ'
				group by id_mes, id_empresa, cod_empresa,
					Allocation) gts
			where ods.id_mes = gts.id_mes
				and ods.Allocation = gts.Allocation) vw
				left outer join #Info_Dir_Grp_Sede inf
					on vw.id_mes = inf.id_mes
						and vw.id_empresa = inf.id_empresa
						and vw.id_cc = inf.id_cc
		/*

		-- select * from #Prorrateo_Global where cod_agrupador = '910100' and cod_empresa = 1184
		-- select * from #Prorrateo_Global where cod_agrupador = '910550' and cod_empresa = 1181
		-- select * from #Prorrateo_Global where cod_agrupador = '702018' and cod_empresa = 1188 and Allocation = 'Training & Consulting'
		-- select * from #Prorrateo_Global where Allocation = 'Training & Consulting' and cod_empresa = 1188
		
		select cod_agrupador, cast(sum(Prorrateo) as float)
		from #Prorrateo_Global 
		where cod_empresa = 1181 and Allocation = 'Gastos HQ' and cod_zona = 'WT'
		group by cod_agrupador
		order by 1
		*/

		-- Excepcion (703920) - 4% Sobre la Venta (String Professional Sales Metro D.F.) - 1188
		insert into #Prorrateo_Global (
			id_mes, id_empresa, id_cc, id_agrupador, id_zona, cod_empresa, cod_cc, 
			cod_agrupador, cod_zona, desc_zona, Allocation, 
			Venta, MB, Venta_Empresa,  MB_Empresa, 
			Venta_Global, MB_Global, Gasto_Global, Gasto_Empresa, 
			Prorrateo, Dir_allocation)
		select id_mes, id_empresa, id_cc, id_agrupador, id_zona, cod_empresa, cod_cc, 
			cod_agrupador, cod_zona, max(desc_zona) as desc_zona, 'Gastos HQ' as Allocation,
			sum(venta) as Venta, 0 as MB, 0 as Venta_Empresa, 0 as MB_Empresa, 
			0 as Venta_Global, 0 as MB_Global, 0 as Gasto_Global, 0 as Gasto_Empresa, 
			sum(venta)*-0.04 as Prorrateo, 0 as Dir_allocation
		from (select ft.id_mes, ft.id_empresa, em.cod_empresa, 
			ods.id_cc, ods.cod_cc, 
			ods.id_agrupador,
			ods.cod_agrupador,
			ods.cod_zona,
			ods.id_zona,
			ods.desc_zona,
			case when left(ln.cod_cuenta,2) = '60' then real else 0 end as Venta
			from dbo.FT_OPR_DETALLE_CUENTA ft,
				dbo.DEMP_LK_EMPRESA em,
				dbo.DESG_LK_LINEA ln,
				(select a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona,
					max(case when b.dir_prorrateo = 'SI' then 1 else 0 end) as CC_Direc
				-- select *
				from dbo.VW_LK_CC_OPR_HIST a
					left outer join dbo.VW_LK_CC_OPR_HIST b
						on a.id_mes = b.id_mes
							and a.cod_agrupador = b.cod_cc
				group by a.id_mes, a.id_cc, a.cod_cc, a.id_agrupador, a.cod_agrupador, a.id_zona, a.cod_zona, a.desc_zona) ods
			where ft.id_mes = ods.id_mes
				and ft.id_cc_orig = ods.id_cc
				and ft.id_empresa = em.id_empresa
				and ft.id_linea = ln.id_linea
				and ft.origen not in ('TOTAL', 'PRORRATEO', 'DSO')
				and left(ln.cod_cuenta,2) in ('60')
				and ft.id_mes = ods.id_mes
				and ft.id_cc_opr = ods.id_cc
				and ods.cod_agrupador in ('703920')
				and em.cod_empresa = '1188'
				and ft.id_mes = @idMes
				) vw
		group by id_mes, id_empresa, id_cc, id_agrupador, id_zona, cod_empresa, cod_cc, 
			cod_agrupador, cod_zona

		-- Restar Gasto SP en Sede (910100) - 1188
		insert into #Prorrateo_Global (
			id_mes, id_empresa, id_cc, id_agrupador, id_zona, cod_empresa, cod_cc, 
			cod_agrupador, cod_zona, desc_zona, Allocation, 
			Venta, MB, Venta_Empresa,  MB_Empresa, 
			Venta_Global, MB_Global, Gasto_Global, Gasto_Empresa, 
			Prorrateo, Dir_allocation)
		select a.id_mes, a.id_empresa, a.id_cc, a.id_agrupador, a.id_zona,
			a.cod_empresa, a.cod_cc, a.cod_agrupador, a.cod_zona, a.desc_zona,
			a.Allocation, 0 as Venta, 0 as MB, 0 as Venta_Empresa, 0 as MB_Empresa,
			0 as Venta_Global, 0 as MB_Global, 0 as Gasto_Global, 
			b.Gasto_Empresa, b.Prorrateo*-1,
			a.Dir_allocation
		from #Prorrateo_Global a, #Prorrateo_Global b
		where a.id_mes  = b.id_mes
			and a.id_empresa = b.id_empresa
			and a.cod_agrupador = '910100'
			and a.cod_empresa = '1188'
			and b.cod_agrupador = '703920'
			
		-- Prorrateo SubZona
		-- drop table #prorrateo_subzona
		select p.id_mes, 'PRORRATEO' as origen, p.id_cc, 0 as id_cliente, 0 as id_subclte,
			p.id_empresa, ln.id_linea, 
			isnull(cc.id_cuenta,0) as id_cuenta,
			isnull(sr.id_tipo_serv_detalle,0) as id_tipo_serv_detalle,
			Prorrateo as Real, 0 as budget, 0 as real_num, 0 as real_den,
			p.cod_empresa, p.id_zona, p.cod_zona, p.Desc_Zona, p.Dir_allocation,
			p.cod_cc, p.id_agrupador, p.cod_agrupador, p.Venta, p.MB, 
			p.Gasto_Empresa as Gasto
		-- select *
		-- select sum(prorrateo)
		into #prorrateo_subzona
		from #Prorrateo_Global p
			left outer join (select Allocation, min(id_linea) as id_linea,
								max(cod_cuenta) as cod_cuenta,
								max(cod_servicio) as cod_servicio
							from (
								select id_linea, case  
									when desc_linea like '%Permanent%' then 'Permanent'
									when desc_linea like '%Trainning%Consulting%' then 'Training & Consulting'
									when desc_linea like '%Op%Industrial%' then 'Gastos Op. Industrial'
									when desc_linea like '%Operaciones%' then 'Operaciones'
									when desc_linea like '%sede%' then 'Gastos HQ'
									end as Allocation,
									desc_linea,
									cod_cuenta,
									cod_servicio
								from DESG_LK_LINEA ln 
								where DESC_LINEA in ('Allocation Gastos Permanent', 'Allocation  Trainning & Consulting',
									'Allocation  Op Industrial', 'Allocation Operaciones', 'Allocation  SEDE')
									and calculo is null
									and flag = 1
									and cod_servicio = 'NA'
									and cod_cuenta like '99%') ln
							group by Allocation) ln
				on ln.Allocation = p.Allocation
					left outer join dbo.DGEN_LK_CTA_CONTABLE cc
						on cc.cod_cuenta = ln.cod_cuenta
					left outer join dbo.DFAC_LK_TIPO_SERVICIO_DETALLE sr
						on sr.cod_tipo_serv_detalle = ln.cod_servicio

		set @msg = 'Prorrateo SubZona: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- select * from #prorrateo_zona
		-- select * from #prorrateo_subzona WHERE COD_ZONA = 'wt'
		-- select * from #allocations al where CC_Direc = 1
		-- select * from #prorrateo_subzona where cod_agrupador = '910500'
		-- return

		-------------------------------------------
		-- CARGA
		-------------------------------------------
		exec etl.prc_Logging @idBatch, '- Carga en DW:'

		begin transaction
		
		delete ft
		from FT_OPR_DETALLE_CUENTA ft
		where ft.id_mes = @idMes
			and ft.origen = 'PRORRATEO'

		set @msg = 'Borrados FT Cble: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg		

		delete ft
		from dbo.FT_OPR_AGRUPADA ft
		where ft.id_mes = @idMes
			and ft.origen = 'PRORRATEO'

		set @msg = 'Borrados FT Agrup: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg		

		delete ft
		from FT_OPR_DETALLE ft
		where ft.id_mes = @idMes
			and ft.origen = 'PRORRATEO'

		set @msg = 'Borrados FT Det: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 

		delete ft
		from FT_OPR ft
		where ft.id_mes = @idMes
			and ft.origen = 'PRORRATEO'

		set @msg = 'Borrados FT: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 
		
		-- Detalle Cuenta
		insert into dbo.FT_OPR_DETALLE_CUENTA (id_mes, origen, id_cc_orig, 
			id_cc_opr, id_cliente, id_subclte, id_empresa, 
			id_linea, id_cuenta, id_tipo_serv_detalle,
			real, budget, real_num, real_den)
		select st.id_mes, st.origen, st.id_cc as id_cc_orig, 
			st.id_cc as id_cc_opr,
			st.id_cliente, st.id_subclte, st.id_empresa, 
			st.id_linea, st.id_cuenta, st.id_tipo_serv_detalle,
			sum(st.real) as real,
			sum(st.budget) as budget,
			sum(isnull(st.real_num,0)), 
			sum(isnull(st.real_den,0))
		from (select id_mes, origen, id_cc, id_cliente, id_subclte, id_empresa, 
				id_linea, id_cuenta, id_tipo_serv_detalle,
				real, budget, real_num, real_den
			from #prorrateo_zona where id_linea > 0
			union 
			select id_mes, origen, id_cc, id_cliente, id_subclte, id_empresa, 
				id_linea, id_cuenta, id_tipo_serv_detalle,
				real, budget, real_num, real_den
			from #prorrateo_subzona where id_linea > 0) st
		group by st.id_mes, st.origen, st.id_cc, 
			st.id_cliente, st.id_subclte, st.id_empresa, 
			st.id_linea, st.id_cuenta, st.id_tipo_serv_detalle
		having not (sum(real) = 0 and sum(budget) = 0)

		set @msg = 'Insertados FT Cble: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Agrupada
		insert into dbo.FT_OPR_AGRUPADA (id_mes, 
			origen, id_cc_opr, id_agrupador, id_empresa, 
			id_linea, id_cuenta, id_tipo_serv_detalle,
			real, budget, real_num, real_den, division)
		select st.id_mes, st.origen, st.id_cc as id_cc_opr,
			st.id_agrupador, st.id_empresa, 
			st.id_linea, st.id_cuenta, st.id_tipo_serv_detalle,
			sum(st.real) as real,
			sum(st.budget) as budget,
			sum(isnull(st.real_num,0)), 
			sum(isnull(st.real_den,0)),
			0 as division
		from (select id_mes, origen, id_cc, id_cliente, id_subclte, id_empresa, 
				id_linea, id_cuenta, id_tipo_serv_detalle,
				real, budget, real_num, real_den,
				id_agrupador
			from #prorrateo_zona where id_linea > 0
			union 
			select id_mes, origen, id_cc, id_cliente, id_subclte, id_empresa, 
				id_linea, id_cuenta, id_tipo_serv_detalle,
				real, budget, real_num, real_den,
				id_agrupador
			from #prorrateo_subzona where id_linea > 0) st
		group by st.id_mes, st.origen, st.id_cc, st.id_agrupador,
			st.id_empresa, st.id_linea, st.id_cuenta, st.id_tipo_serv_detalle
		having not (sum(real) = 0 and sum(budget) = 0)

		set @msg = 'Insertados FT Agrup: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg

		-- Detalle
		insert into dbo.FT_OPR_DETALLE (id_mes, origen,
			id_cc_orig, id_cc_opr, id_cliente, 
			id_subclte, id_empresa, id_linea, 
			real, budget)
		select st.id_mes, st.origen,
			st.id_cc as id_cc_orig, 
			st.id_cc as id_cc_opr,
			st.id_cliente, st.id_subclte, st.id_empresa, 
			st.id_linea, 
			sum(st.real) as real,
			sum(st.budget) as budget
		from (select id_mes, origen, id_cc, id_cliente, id_subclte, id_empresa, 
				id_linea, id_cuenta, id_tipo_serv_detalle,
				real, budget, real_num, real_den
			from #prorrateo_zona where id_linea > 0
			union 
			select id_mes, origen, id_cc, id_cliente, id_subclte, id_empresa, 
				id_linea, id_cuenta, id_tipo_serv_detalle,
				real, budget, real_num, real_den
			from #prorrateo_subzona where id_linea > 0) st
		group by st.id_mes, st.origen, st.id_cc, 
			st.id_cliente, st.id_subclte, st.id_empresa, st.id_linea
		having not (sum(real) = 0 and sum(budget) = 0)

		set @msg = 'Insertados FT Det: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 

		-- OPT
		insert into FT_OPR (id_mes, origen,
			id_cc_orig, id_cc_opr, id_linea, 
			real, budget)
		select st.id_mes, st.origen,
			st.id_cc as id_cc_orig, 
			st.id_cc as id_cc_opr,
			st.id_linea, 
			sum(st.real) as real,
			sum(st.budget) as budget
		from (select id_mes, origen, id_cc, id_cliente, id_subclte, id_empresa, 
				id_linea, id_cuenta, id_tipo_serv_detalle,
				real, budget, real_num, real_den
			from #prorrateo_zona where id_linea > 0
			union 
			select id_mes, origen, id_cc, id_cliente, id_subclte, id_empresa, 
				id_linea, id_cuenta, id_tipo_serv_detalle,
				real, budget, real_num, real_den
			from #prorrateo_subzona where id_linea > 0) st
		group by st.id_mes, st.origen, st.id_cc, st.id_linea
		having not (sum(real) = 0 and sum(budget) = 0)

		set @msg = 'Insertados FT: ' + convert(varchar, @@rowcount); exec etl.prc_Logging @idBatch, @msg 

		commit transaction

	end try
	begin catch
		if xact_state() <> 0
			rollback transaction

		exec etl.prc_log_error_sp 'MAPPING', @idBatch

	end catch
end
GO
/*
exec etl.map_ft_opr_prorrateo 0, 202201
exec etl.map_ft_opr_prorrateo 0, 202202
exec etl.map_ft_opr_prorrateo 0, 202203
*/
