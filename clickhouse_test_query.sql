-- Количество пар по видам занятий у каждого преподавателя
SELECT
    lecturer_name,
    lecturer_rank,
    kind_of_work,
    count() AS lessons_count
FROM rasp_omgtu.schedule
WHERE partition_date BETWEEN '2026-05-13' AND '2026-05-31'
GROUP BY
    lecturer_name,
    lecturer_rank,
    kind_of_work
ORDER BY
    lecturer_name,
    lessons_count DESC;