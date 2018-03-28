'use strict';

var app = angular.module('guild', ['ngSanitize', 'ui.select']);

/**
 * AngularJS default filter with the following expression:
 * "person in people | filter: {name: $select.search, age: $select.search}"
 * performs a AND between 'name: $select.search' and 'age: $select.search'.
 * We want to perform a OR.
 */
app.filter('propsFilter', function() {
  return function(items, props) {
    var out = [];

    if (angular.isArray(items)) {
      items.forEach(function(item) {
        var itemMatches = false;

        var keys = Object.keys(props);
        for (var i = 0; i < keys.length; i++) {
          var prop = keys[i];
          var text = props[prop].toLowerCase();
          if (item[prop].toString().toLowerCase().indexOf(text) !== -1) {
            itemMatches = true;
            break;
          }
        }

        if (itemMatches) {
          out.push(item);
        }
      });
    } else {
      // Let the output be the input untouched
      out = items;
    }

    return out;
  };
});

app.controller('GuildCtrl', function($scope, $http, $timeout) {
  $scope.disabled = undefined;
  $scope.searchEnabled = undefined;

  $scope.enable = function() {
    $scope.disabled = false;
  };

  $scope.disable = function() {
    $scope.disabled = true;
  };

  $scope.enableSearch = function() {
    $scope.searchEnabled = true;
  }

  $scope.disableSearch = function() {
    $scope.searchEnabled = false;
  }

  $scope.counter = 0;
  $scope.someFunction = function (item, model){
    $scope.counter++;
    $scope.eventResult = {item: item, model: model};
  };

  $scope.availableTags = [];

  $http.get('tags.json').success(function(data){
    $scope.availableTags = data.tags;
  });

  $scope.search = {};
  $scope.search.Tags = ['php'];

  // API Call Code

  var pendingTask;

  if ($scope.search.Tags) {
      fetch();
    }

  $scope.change = function() {
      if (pendingTask) {
        clearTimeout(pendingTask);
      }
      pendingTask = setTimeout(fetch, 800);
    };

    function fetch() {

      var tags = $scope.search.Tags

      var query = "";

      for(var i=0; i<tags.length; i++){
          query = query+'tag='+tags[i];
          if(i!=tags.length-1)
          {
            query =query+'&';
          }
      }

      $http.get("http://hadoop.rcg.sfu.ca:5000/?" + query)
        .success(function(response) {
          $scope.users = response.users;
          console.log(response.users);
        });


    }

});
