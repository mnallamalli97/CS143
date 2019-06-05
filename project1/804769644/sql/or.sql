--or case 

select sname, A.aname, url
from project1.tfidf L
inner join 
	project1.song R on L.sid = R.sid
inner join 
	project1.artist A on R.aid = A.aid
where 
	token = 'she' or token='could'
group by 
	L.sid, sname, A.aid, url
order by 
	sum(tfidf) desc;


