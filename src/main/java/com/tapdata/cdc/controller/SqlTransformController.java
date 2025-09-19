package com.tapdata.cdc.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * SQL Transform Controller
 * Serves the SQL transformation tool HTML page
 */
@Controller
public class SqlTransformController {

    /**
     * Serve the SQL transformation tool page
     * The HTML file is located at: src/main/resources/static/sql-transform.html
     *
     * @return redirect to the static HTML page
     */
    @GetMapping("/sql-transform")
    public String sqlTransformPage() {
        // Spring Boot will automatically serve static content from /static folder
        // We just need to redirect to the HTML file
        return "redirect:/sql-transform.html";
    }
}