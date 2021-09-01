import './App.css';
import 'antd/dist/antd.css';
import React from 'react';
import {Switch, Route, Redirect} from 'react-router-dom';
import {adminRoutes} from "./routes";
import Frame from './components/Frame/Index'
import {isLogined} from "./utils/auth";

function App() {
  return isLogined() ? (
      <Frame>
        <Switch>
          {adminRoutes.map(route => {
            return (
                <Route
                    key={route.path}
                    path={route.path}
                    exact={route.exact}
                    render={routeProps => {
                      document.title = route.title || "儒猿分布式文件系统";
                      return <route.component {...routeProps} />
                    }}
                />
            );
          })}
          <Redirect to={adminRoutes[0].path} from="/admin"/>
          <Redirect to='/404'/>
        </Switch>
      </Frame>) : (<Redirect to="/login"/>);
}

export default App;
