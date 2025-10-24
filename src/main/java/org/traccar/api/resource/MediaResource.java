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

@Path("mediafiles") // hanya "mediafiles", bukan "public/mediafiles"
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

        List<Map<String, Object>> photos = new ArrayList<>();
        try (DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(deviceFolder, "*.jpg")) {
            for (java.nio.file.Path file : stream) {
                String name = file.getFileName().toString();
                String tsRaw = name.replace(".jpg", "");
                LocalDateTime ts;
                try {
                    ts = LocalDateTime.parse(tsRaw, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                } catch (Exception e) { continue; }

                Map<String,Object> item = new LinkedHashMap<>();
                item.put("fileName", name);
                item.put("timestamp", ts.toString());
                item.put("url", "/api/media/" + uniqueId + "/" + name);
                photos.add(item);
            }
        } catch (IOException e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        photos.sort((a, b) -> ((String)b.get("timestamp")).compareTo((String)a.get("timestamp")));
        return Response.ok(photos).build();
    }
}
