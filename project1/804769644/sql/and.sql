--and case 

select sname, A.aname, url
from project1.tfidf L
inner join 
	project1.song R on L.sid = R.sid
inner join 
	project1.artist A on R.aid = A.aid
where 
	token = 'girl' or token='on' or token='fire'
group by 
	L.sid, sname, A.aid, url
having 
	count( distinct token) = 3
order by 
	sum(tfidf) desc;


