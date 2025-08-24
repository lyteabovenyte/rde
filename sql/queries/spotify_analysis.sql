-- Spotify Audio Features Analysis Queries

-- 1. Table overview
SELECT COUNT(*) as total_tracks,
       AVG(danceability) as avg_danceability,
       AVG(energy) as avg_energy,
       AVG(valence) as avg_valence,
       AVG(tempo) as avg_tempo,
       MIN(duration_ms) as shortest_track_ms,
       MAX(duration_ms) as longest_track_ms
FROM iceberg.default.spotify_audio_features_simple;

-- 2. Music mood analysis
SELECT 
    CASE 
        WHEN valence >= 0.7 THEN 'Very Positive'
        WHEN valence >= 0.5 THEN 'Positive'
        WHEN valence >= 0.3 THEN 'Neutral'
        ELSE 'Negative'
    END as mood,
    COUNT(*) as track_count,
    AVG(energy) as avg_energy,
    AVG(danceability) as avg_danceability,
    AVG(tempo) as avg_tempo
FROM iceberg.default.spotify_audio_features_simple
GROUP BY 1
ORDER BY MIN(valence) DESC;

-- 3. Energy vs Danceability correlation
SELECT 
    CASE 
        WHEN energy >= 0.7 THEN 'High Energy'
        WHEN energy >= 0.4 THEN 'Medium Energy'
        ELSE 'Low Energy'
    END as energy_level,
    CASE 
        WHEN danceability >= 0.7 THEN 'Highly Danceable'
        WHEN danceability >= 0.5 THEN 'Danceable'
        ELSE 'Not Danceable'
    END as dance_level,
    COUNT(*) as tracks,
    AVG(tempo) as avg_tempo,
    AVG(valence) as avg_positivity
FROM iceberg.default.spotify_audio_features_simple
GROUP BY 1, 2
ORDER BY 1, 2;

-- 4. Tempo distribution
SELECT 
    CASE 
        WHEN tempo < 80 THEN 'Slow (<80 BPM)'
        WHEN tempo < 120 THEN 'Medium (80-120 BPM)'
        WHEN tempo < 160 THEN 'Fast (120-160 BPM)'
        ELSE 'Very Fast (>160 BPM)'
    END as tempo_range,
    COUNT(*) as tracks,
    AVG(energy) as avg_energy,
    AVG(danceability) as avg_danceability
FROM iceberg.default.spotify_audio_features_simple
GROUP BY 1
ORDER BY MIN(tempo);

-- 5. Musical key analysis
SELECT key as musical_key,
       COUNT(*) as tracks,
       AVG(valence) as avg_positivity,
       AVG(energy) as avg_energy
FROM iceberg.default.spotify_audio_features_simple
GROUP BY key
ORDER BY tracks DESC;

-- 6. Track duration analysis
SELECT 
    CASE 
        WHEN duration_ms < 120000 THEN 'Short (<2min)'
        WHEN duration_ms < 240000 THEN 'Medium (2-4min)'
        WHEN duration_ms < 360000 THEN 'Long (4-6min)'
        ELSE 'Very Long (>6min)'
    END as duration_category,
    COUNT(*) as tracks,
    AVG(energy) as avg_energy,
    AVG(danceability) as avg_danceability,
    AVG(valence) as avg_valence
FROM iceberg.default.spotify_audio_features_simple
GROUP BY 1
ORDER BY MIN(duration_ms);
