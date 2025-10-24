package org.traccar.api.resource;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom resource untuk menampilkan daftar foto (media) yang disimpan Traccar
 * Kompatibel dengan MediaFilter & MediaManager kamu
 */
@Path("media")
@Produces(MediaType.APPLICATION_JSON)
public class MediaResource {

    // Path penyimpanan media di server kamu
    private static final java.nio.file.Path MEDIA_ROOT = java.nio.file.Paths.get("/opt/traccar/media");

    @GET
    @Path("{uniqueId}")
    public Response listMedia(
            @PathParam("uniqueId") String uniqueId,
            @QueryParam("from") String from,
            @QueryParam("to") String to) {

        java.nio.file.Path deviceFolder = MEDIA_ROOT.resolve(uniqueId);
        if (!Files.exists(deviceFolder)) {
            return Response.ok(Collections.emptyList()).build();
        }

        LocalDateTime fromTime = null;
        LocalDateTime toTime = null;

        try {
            if (from != null && to != null) {
                fromTime = LocalDateTime.parse(from, DateTimeFormatter.ISO_DATE_TIME);
                toTime = LocalDateTime.parse(to, DateTimeFormatter.ISO_DATE_TIME);
            }
        } catch (Exception ignored) {}

        List<Map<String, Object>> photos = new ArrayList<>();

        try (DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(deviceFolder, "*.jpg")) {
            for (java.nio.file.Path file : stream) {
                String fileName = file.getFileName().toString();
                String timestampStr = fileName.replace(".jpg", "");

                // Nama file = yyyyMMddHHmmss
                LocalDateTime ts;
                try {
                    ts = LocalDateTime.parse(timestampStr, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                } catch (Exception e) {
                    continue;
                }

                // Filter waktu
                if (fromTime != null && toTime != null) {
                    if (ts.isBefore(fromTime) || ts.isAfter(toTime)) {
                        continue;
                    }
                }

                Map<String, Object> item = new LinkedHashMap<>();
                item.put("fileName", fileName);
                item.put("timestamp", ts.toString());
                item.put("url", "/api/media/" + uniqueId + "/" + fileName);
                photos.add(item);
            }
        } catch (IOException e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        // Urutkan dari terbaru ke terlama
        photos.sort((a, b) -> ((String) b.get("timestamp")).compareTo((String) a.get("timestamp")));
        return Response.ok(photos).build();
    }
}
