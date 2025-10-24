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
import java.util.*;

/**
 * Custom endpoint publik untuk menampilkan daftar foto dari Traccar
 * Path endpoint: /api/mediafiles/{uniqueId}?from=...&to=...
 * Tidak membutuhkan login session (tidak difilter oleh MediaFilter)
 */
@Path("public/mediafiles")
@Produces(MediaType.APPLICATION_JSON)
public class MediaResource {

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
                LocalDateTime ts;
                try {
                    ts = LocalDateTime.parse(timestampStr, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                } catch (Exception e) {
                    continue;
                }

                if (fromTime != null && toTime != null) {
                    if (ts.isBefore(fromTime) || ts.isAfter(toTime)) {
                        continue;
                    }
                }

                Map<String, Object> item = new LinkedHashMap<>();
                item.put("fileName", fileName);
                item.put("timestamp", ts.toString());
                // gunakan endpoint bawaan untuk foto
                item.put("url", "/api/media/" + uniqueId + "/" + fileName);
                photos.add(item);
            }
        } catch (IOException e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        photos.sort((a, b) -> ((String) b.get("timestamp")).compareTo((String) a.get("timestamp")));
        return Response.ok(photos).build();
    }
}
