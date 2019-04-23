package org.superbiz.moviefun.albums;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import javax.sql.DataSource;

@Configuration
@EnableAsync
@EnableScheduling
public class AlbumsUpdateScheduler {

    private static final long SECONDS = 1000;
    private static final long MINUTES = 60 * SECONDS;

    private final AlbumsUpdater albumsUpdater;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final JdbcTemplate jdbcTemplate;



    public AlbumsUpdateScheduler(DataSource dataSource, AlbumsUpdater albumsUpdater) {
        this.albumsUpdater = albumsUpdater;
        this.jdbcTemplate =  new JdbcTemplate(dataSource);
    }


    @Scheduled(initialDelay = 15 * SECONDS, fixedRate = 2 * MINUTES)
    public void run() {
        try {
            if (startAlbumSchedulerTask()) {
                logger.debug("Starting albums update");
                albumsUpdater.update();
                logger.debug("Finished albums update");

            } else {
                logger.debug("Nothing to start");
            }

        } catch (Throwable e) {
            logger.error("Error while updating albums", e);
        }
    }


    private boolean startAlbumSchedulerTask() {
        int updatedRows = jdbcTemplate.update(
                "UPDATE album_scheduler_task" +
                        " SET started_at = now()" +
                        " WHERE started_at IS NULL" +
                        " OR started_at < date_sub(now(), INTERVAL 2 MINUTE)"
        );

        return updatedRows > 0;
    }

    /*public boolean startAlbumSchedulerTask() {

        int timeDiffInMins =  jdbcTemplate.queryForObject("SELECT TIMESTAMPDIFF(MINUTE,started_at,SYSDATE()) FROM album_scheduler_task",Integer.class);
        boolean start = (timeDiffInMins >= 2) ? true:false;
        if(start) jdbcTemplate.update("UPDATE album_scheduler_task" +
                                                "SET started_at = now()");
        return start;

    }*/




}
