/*
 * Copyright 2025 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.api.publicendpoint;

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
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Endpoint publik (tanpa login) untuk daftar foto dari perangkat kamera.
 * Listing: GET /public/mediafiles/{uniqueId}?from=...&to=...
 * File publik: /public/media/{uniqueId}/{fileName}.jpg (dilayani ResourceServlet di WebServer)
 */
@Path("mediafiles")
@Produces(MediaType.APPLICATION_JSON)
public class MediaResource {

    private static final DateTimeFormatter FILE_NAME_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private static volatile java.nio.file.Path mediaRoot = Paths.get("media");

    public static void setMediaRoot(java.nio.file.Path root) {
        mediaRoot = root;
    }

    @GET
    @Path("{uniqueId}")
    public Response listMedia(
            @PathParam("uniqueId") String uniqueId,
            @QueryParam("from") String from,
            @QueryParam("to") String to) {

        java.nio.file.Path deviceFolder = mediaRoot.resolve(uniqueId).normalize();
        if (!deviceFolder.startsWith(mediaRoot) || !Files.exists(deviceFolder)) {
            return Response.ok(Collections.emptyList()).build();
        }

        LocalDateTime fromTime = null;
        LocalDateTime toTime = null;
        try {
            if (from != null && to != null) {
                fromTime = LocalDateTime.parse(from, DateTimeFormatter.ISO_DATE_TIME);
                toTime = LocalDateTime.parse(to, DateTimeFormatter.ISO_DATE_TIME);
            }
        } catch (Exception ignored) {
            fromTime = null;
            toTime = null;
        }

        List<Map<String, Object>> photos = new ArrayList<>();

        try (DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(deviceFolder, "*.jpg")) {
            for (java.nio.file.Path file : stream) {
                String fileName = file.getFileName().toString();
                String timestampString = fileName.replace(".jpg", "");

                LocalDateTime timestamp;
                try {
                    timestamp = LocalDateTime.parse(timestampString, FILE_NAME_FORMAT);
                } catch (Exception e) {
                    continue;
                }

                if (fromTime != null && toTime != null
                        && (timestamp.isBefore(fromTime) || timestamp.isAfter(toTime))) {
                    continue;
                }

                Map<String, Object> item = new LinkedHashMap<>();
                item.put("fileName", fileName);
                item.put("timestamp", timestamp.toString());
                item.put("url", "/public/media/" + uniqueId + "/" + fileName);
                photos.add(item);
            }
        } catch (IOException e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        photos.sort((a, b) -> ((String) b.get("timestamp")).compareTo((String) a.get("timestamp")));
        return Response.ok(photos).build();
    }

}
