select pixel_color, count(pixel_color) as changes
from pixel_data
where changed_at between '2022-04-01 12:00:00' and '2022-04-01 18:00:00'
group by pixel_color
order by changes desc
