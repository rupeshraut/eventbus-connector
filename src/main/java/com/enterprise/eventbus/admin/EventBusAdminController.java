package com.enterprise.eventbus.admin;

import com.enterprise.eventbus.config.TopicBindingProperties;
import com.enterprise.eventbus.container.ContainerRegistration;
import com.enterprise.eventbus.dlq.DltStatsService;
import com.enterprise.eventbus.model.ContainerState;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/eventbus")
public class EventBusAdminController {

    private final EventBusAdmin admin;

    public EventBusAdminController(EventBusAdmin admin) {
        this.admin = admin;
    }

    @PostMapping("/bindings/{name}")
    public ResponseEntity<Map<String, String>> register(@PathVariable String name,
                                                         @RequestBody TopicBindingProperties config) {
        admin.register(name, config);
        return ResponseEntity.ok(Map.of("status", "registered", "binding", name));
    }

    @DeleteMapping("/bindings/{name}")
    public ResponseEntity<Map<String, String>> destroy(@PathVariable String name) {
        admin.destroy(name);
        return ResponseEntity.ok(Map.of("status", "destroyed", "binding", name));
    }

    @PostMapping("/bindings/{name}/start")
    public ResponseEntity<Map<String, String>> start(@PathVariable String name) {
        admin.start(name);
        return ResponseEntity.ok(Map.of("status", "started", "binding", name));
    }

    @PostMapping("/bindings/{name}/stop")
    public ResponseEntity<Map<String, String>> stop(@PathVariable String name) {
        admin.stop(name);
        return ResponseEntity.ok(Map.of("status", "stopped", "binding", name));
    }

    @PostMapping("/bindings/{name}/pause")
    public ResponseEntity<Map<String, String>> pause(@PathVariable String name) {
        admin.pause(name);
        return ResponseEntity.ok(Map.of("status", "paused", "binding", name));
    }

    @PostMapping("/bindings/{name}/resume")
    public ResponseEntity<Map<String, String>> resume(@PathVariable String name) {
        admin.resume(name);
        return ResponseEntity.ok(Map.of("status", "resumed", "binding", name));
    }

    @GetMapping("/bindings")
    public ResponseEntity<List<BindingSummary>> listBindings() {
        var summaries = admin.listBindings().stream()
                .map(r -> new BindingSummary(r.bindingName(), r.config().getTopic(),
                        r.state(), r.config().getListenerType().name(),
                        r.circuitBreaker() != null ? r.circuitBreaker().getState().name() : "N/A"))
                .toList();
        return ResponseEntity.ok(summaries);
    }

    @GetMapping("/bindings/{name}")
    public ResponseEntity<BindingSummary> getBinding(@PathVariable String name) {
        var reg = admin.listBindings().stream()
                .filter(r -> r.bindingName().equals(name)).findFirst()
                .orElseThrow();
        return ResponseEntity.ok(new BindingSummary(reg.bindingName(), reg.config().getTopic(),
                reg.state(), reg.config().getListenerType().name(),
                reg.circuitBreaker() != null ? reg.circuitBreaker().getState().name() : "N/A"));
    }

    // -- DLT endpoints --

    @PostMapping("/bindings/{name}/dlt/replay")
    public ResponseEntity<Map<String, Object>> replayDlt(@PathVariable String name,
                                                          @RequestParam(defaultValue = "10") int count) {
        int replayed = admin.replayDlt(name, count);
        return ResponseEntity.ok(Map.of("replayed", replayed, "binding", name));
    }

    @PostMapping("/bindings/{name}/dlt/replay-to-tier")
    public ResponseEntity<Map<String, Object>> replayToTier(@PathVariable String name,
                                                              @RequestParam int tier,
                                                              @RequestParam(defaultValue = "10") int count) {
        int replayed = admin.replayDltToTier(name, tier, count);
        return ResponseEntity.ok(Map.of("replayed", replayed, "binding", name, "tier", tier));
    }

    @GetMapping("/bindings/{name}/dlt/stats")
    public ResponseEntity<DltStatsService.DltStats> dltStats(@PathVariable String name) {
        return ResponseEntity.ok(admin.dltStats(name));
    }

    @DeleteMapping("/bindings/{name}/dlt")
    public ResponseEntity<Map<String, Object>> purgeDlt(@PathVariable String name,
                                                         @RequestParam String olderThan) {
        long deleted = admin.purgeDlt(name, Instant.parse(olderThan));
        return ResponseEntity.ok(Map.of("deleted", deleted, "binding", name));
    }

    record BindingSummary(String bindingName, String topic, ContainerState state,
                          String listenerType, String circuitBreakerState) {}
}
