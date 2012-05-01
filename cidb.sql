begin;

drop table if exists contractor cascade;

-- Gah, the mixing of English and Malay annoys me
create table contractor (
    cont_id int not null primary key,
    ctime timestamp,
    company text,
    address text,
    email text,
    telephone text,
    fax text,
    postcode text,
    source text,
    status text,
    town text,

    cidb_nopendaftaran text,
    cidb_bumiputera text,
    cidb_pengkhususan text,
    cidb_tarikhluputpendaftaran date,
    cidb_vgred text,

    pkk_nopendaftaran text,
    pkk_kelas text,
    
    rob text,
    roc text,
    tradinglicense text,
    modaldibenarkan numeric,
    modalberbayar numeric
);

--    json_data text,

drop table if exists project cascade;
create table project (
	project_id serial primary key,
	cont_id int not null references contractor(cont_id) on delete cascade on update cascade,
	tajuk text,
	tarikh_anugerah date,
	nilai numeric,
	klien text
);
create index project_idx_cont_id on project (cont_id);
	
drop table if exists director cascade;
create table director (
	director_id serial primary key,
	cont_id int not null references contractor(cont_id) on delete cascade on update cascade,
	nama text,
	jawatan text,
	warganegara text
);
create index director_idx_cont_id on director (cont_id);


create view project_view as
   SELECT c.cont_id, c.company, p.tajuk, p.nilai, p.tarikh_anugerah
     FROM project p
     JOIN contractor c USING (cont_id)
 ORDER BY c.company, p.tarikh_anugerah;

create view director_view as
   SELECT c.cont_id, c.company, d.nama, d.jawatan, d.warganegara
     FROM contractor c
     JOIN director d USING (cont_id)
 ORDER BY c.company;

commit;
