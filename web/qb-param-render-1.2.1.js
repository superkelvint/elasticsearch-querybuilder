Handlebars.registerHelper('render_param', function (prefix, name, type, id, namedObject) {
  var fullname = namedObject ? prefix + "-@"+prefix+"@-" + name : prefix + "-" + name;
  var s = '';

  if (type == 'BOOLEAN') {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>true</option><option>false</option></select>';
  } else if (name == 'type' && prefix.endsWith('multi_match')) {
    s = '<select class="ref form-control" name="' + fullname + '">' +
        '<option/>' +
        '<option>best_fields</option><option>most_fields</option>' +
        '<option>cross_fields</option><option>phrase</option>' +
        '<option>phrase_prefix</option>' +
        '</select>';
  } else if (name == 'type' && prefix.endsWith('multi_match')) {
    s = '<select class="ref form-control" name="' + fullname + '">' +
        '<option/>' +
        '<option>boolean</option><option>phrase</option>' +
        '<option>phrase_prefix</option>' +
        '</select>';
  } else if (type == 'FUZZINESS') {
    s = '<select class="ref form-control" name="' + fullname + '">' +
        '<option/>' +
        '<option>0</option><option>1</option>' +
        '<option>2</option><option>AUTO</option>' +
        '</select>';
  } else if (name == "zero_terms_query") {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>NONE</option><option>ALL</option></select>';
  } else if (name == "score_mode" && prefix.endsWith('nested')) {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>avg</option><option>total</option><option>max</option><option>none</option></select>';
  } else if (name == "score_type" && prefix.endsWith('has_child')) {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>max</option><option>sum</option><option>avg</option><option>none</option></select>';
  } else if (name == "score_type" && prefix.endsWith('has_parent')) {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>score</option><option>none</option></select>';
  } else if (type == 'LIST') {
    s = '<input type="text" placeholder="Comma- or pipe-separated values e.g. car|train|truck" class="ref form-control list_values" prefix="'+fullname+'" id="'+fullname+'_list_values">';
    s += '<div class="list_valuesContainer"></div>'
  } else if (type == 'MAP') {
    s = '<input type="text" placeholder="Comma- or pipe-separated key-values e.g. key1|value1|key2|value2" class="ref form-control map_values" prefix="'+fullname+'" id="'+fullname+'_map_values">';
    s += '<div class="map_valuesContainer"></div>'
  } else if (type == 'OPERATOR') {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>OR</option><option>AND</option></select>';
  } else if (type == 'GEO_DISTANCE') {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>plane</option><option>arc</option><option>sloppy_arc</option><option>factor</option></select>';
  } else if (type == 'SPATIAL_STRATEGY') {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>term</option><option>recursive</option></select>';
  } else if (type == 'SHAPE_BUILDER') {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>point</option><option>multipoint</option><option>linestring</option><option>multilinestring</option><option>polygon</option><option>multipolygon</option><option>envelope</option><option>circle</option></select>';
  } else if (type == 'SHAPE_RELATION') {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>intersects</option><option>disjoint</option><option>within</option></select>';
  } else if (type == 'OPTIMIZE_BBOX') {
    s = '<select class="ref form-control" name="' + fullname + '"><option/><option>memory</option><option>indexed</option><option>none</option></select>';
  } else if (type == 'LIST_FILTER_BUILDER') {
    s = '<input type="button" class="form-control btn btn-info queryadd" counter="0" target="'+fullname+'" targettype="filter" tgid="'+id+'" value="Add filter to '+ name+ '" id="'+fullname+'_addfilter"><ol></ol>';
  } else if (type == 'LIST_QUERY_BUILDER') {
    s = '<input type="button" class="form-control btn btn-info queryadd" counter="0" target="'+fullname+'" targettype="query" tgid="'+id+'" value="Add query to '+ name+ '" id="'+fullname+'_addquery"><ol></ol>';
  } else if (type == 'QUERY_BUILDER') {
    var tgid = rand_id();
    s = "<select _name='" + fullname + "' class='qb form-control' tgid=" + tgid + " type='query' id='"+fullname+"_query'>" + $('.qb').html() + "</select>";
    s += '<div class="qbfields" id="qbfields_' + tgid + '"></div><br/>';
  } else if (type == 'FILTER_BUILDER') {
    var tgid = rand_id();
    s = "<select _name='" + fullname + "' class='qb form-control' tgid=" + tgid + " type='filter' id='"+fullname+"_filter'>" + $('.fb').html() + "</select>";
    s += '<div class="qbfields" id="qbfields_' + tgid + '"></div><br/>';
  } else {
    s = '<input type="text" class="ref form-control" name="' + fullname + '" id="' + fullname + '" ttype="'+type+'">';
  }
  return s;
});