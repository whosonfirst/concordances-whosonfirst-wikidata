

## 1:N   same wikidataid -- multiple WOF record 


#### cebuano  -  ceb_x_preferred 



Result of the bad "ceb_x_preferred"  names:
1 wikidataid  :  28 wof records

```sql
   wd_id   |     id     |     metatable     |           wof_name            | wof_country |               _matching_category                
-----------+------------+-------------------+-------------------------------+-------------+-------------------------------------------------
 Q10987378 |   85799695 | wof_neighbourhood | Albisrieden                   | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85799719 | wof_neighbourhood | Altstetten                    | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85799725 | wof_neighbourhood | Aussersihl                    | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85799955 | wof_neighbourhood | Enge                          | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85799981 | wof_neighbourhood | Fluntern                      | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800057 | wof_neighbourhood | Hirslanden                    | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800071 | wof_neighbourhood | Höngg                         | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800077 | wof_neighbourhood | Hottingen                     | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800361 | wof_neighbourhood | Oberstrass                    | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800373 | wof_neighbourhood | Oerlikon                      | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800509 | wof_neighbourhood | Schwamendingen                | DE          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800527 | wof_neighbourhood | Seebach                       | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800585 | wof_neighbourhood | Unterstrass                   | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800643 | wof_neighbourhood | Wipkingen                     | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800653 | wof_neighbourhood | Witikon                       | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |   85800667 | wof_neighbourhood | Wollishofen                   | CH          | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |  420782353 | wof_neighbourhood | Saatlen                       |             | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |  420782359 | wof_neighbourhood | Sihlfeld                      |             | OK-REP:suggested for replace-N2Label-name-match
 Q10987378 |  420782381 | wof_neighbourhood | Lindenhof                     |             | OK-VAL:validated-N1Full-name-match
 Q10987378 | 1125818533 | wof_locality      | Lindenhof                     | CH          | OK-ADD:suggested for add-N1Full-name-match
 Q10987378 | 1126064617 | wof_neighbourhood | Zürich (Kreis 1) / Lindenhof  | CH          | OK-ADD:suggested for add-N2Label-name-match
 Q10987378 | 1126110389 | wof_locality      | Lindenhof.                    | CH          | OK-ADD:suggested for add-N5JaroWinkler-match
 Q10987378 | 1126110471 | wof_locality      | Lindenhof                     | CH          | OK-ADD:suggested for add-N1Full-name-match
 Q10987378 | 1126111323 | wof_locality      | Lindenhof                     | CH          | OK-ADD:suggested for add-N1Full-name-match
 Q10987378 | 1126112161 | wof_locality      | Lindenhof                     | CH          | OK-ADD:suggested for add-N1Full-name-match
 Q10987378 | 1126112363 | wof_locality      | Lindenhof                     | CH          | OK-ADD:suggested for add-N1Full-name-match
 Q10987378 | 1126113187 | wof_locality      | Lindenhof                     | CH          | OK-ADD:suggested for add-N1Full-name-match
 Q10987378 | 1126113483 | wof_locality      | Lindenhof                     | CH          | OK-ADD:suggested for add-N1Full-name-match
(28 rows)
```


for example : https://whosonfirst.mapzen.com/spelunker/id/85800071/
*
```
als_x_preferred=	Höngg
ceb_x_preferred=	Zürich                  <---- Bad name
deu_x_colloquial=	Hoengg, Hongg
deu_x_preferred=	Höngg
ita_x_preferred=	Höngg
```

Wikidata side:  https://www.wikidata.org/wiki/Q10987378 ( 2017-12-18 )
```
* German  = "Lindenhof"
* Cebuano = "Zürich"   <----   Bad name 
* English = "Lindenhof"
```

Problems:   there are lot of bad "ceb_x_preferred"  labels imported to  wof,

Suggestions:  
- [ ] (wof) double check ( clean ) current "ceb_x_preferred"  - names
- [ ] (me) not use "ceb_x_preferred" names in  the matching algorithm   
- [ ] (me) temporary not import to wof "ceb_x_preferred" - names , wait for another yerar, and wait for better data quality.
