if (window.console) {
    console.log("Welcome to your Play application's JavaScript!");
}

$( document ).ready(function() {

    $(".table_link").on("click", function() {
        var table = $(this).html();
        if(table === "Lenses") {
            table = "MIMIR_LENSES";
        }

        var query = "SELECT * FROM "+table+";";

        $("#query_textarea").val(query);
        $("#query_btn").trigger("click");
    });

    $(".db_link").on("click", function() {
        var db = $(this).html();
        var curr = $("#curr_db").html().trim();

        if(db.valueOf() !== curr.valueOf()) {
            $("#db_field").val(db);
            $("#query_textarea").val("");
            $("#query_btn").trigger("click");
        }
    });

    $("#create_database").on("click", function() {
        var db = prompt("Please enter a name for the new database", "awesomedb");
        var existing_dbs = new Array();

        if(!db.match(/^\w+$/))
            alert("That is not a valid name, please try again");

        db += ".db";

        $(".db_link").each(function() {
            existing_dbs.push($(this).html());
        });

        if($.inArray(db, existing_dbs) != -1) {
            alert("A database with the name "+db+" already exists");
        }
        else {
            window.location.href = "/createDB?db="+db;
        }
    });

    $("#result_table").colResizable( {
        liveDrag: true,
        minWidth: 80
    });

    $("#about_btn").on("click", function() {
        $("#about").toggle(100);
    });

    $("#upload").on("click", function() {
        $("#drop_area").toggle(100);
    });

    $(".close_btn").on("click", function() {
        $(this).parent().hide(100);
    });

    $(".non_deterministic_cell").tooltipster({
        animation: 'fade',
        delay: 200,
        content: $('<h5>Bounds</h5><h6>Minimum 1 Maximum 4<h6>'),
        theme: 'tooltipster-punk',
        position: 'bottom-left',
        minWidth: 250,
    });

    Dropzone.options.myAwesomeDropzone = {
      maxFilesize: 2, // MB
      acceptedFiles: ".csv",
      addRemoveLinks: true,
      init: function() {
        this.on("error", function() {
            var span = $("span[data-dz-errormessage]");
            span.html("Something went wrong! Possible reasons include (i) You are working with the wrong database? (ii) The schema for the table has not yet been loaded? (iii) The name of the csv file does not match the name of the table?");
        });
      }
    };

});