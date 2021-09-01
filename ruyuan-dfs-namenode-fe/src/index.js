import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import reportWebVitals from './reportWebVitals';
import {HashRouter as Router, Switch, Route, Redirect} from 'react-router-dom';
import {mainRoutes} from "./routes";
import App from './App'
import {ConfigProvider} from 'antd';
import zhCN from 'antd/es/locale/zh_CN';

ReactDOM.render(
    <Router>
        <Switch>
            <Route path='/admin'
                   render={routeProps => <ConfigProvider locale={zhCN}><App {...routeProps}/></ConfigProvider>}/>
            {mainRoutes.map(route => {
                return <Route key={route.path} {...route}/>;
            })}
            <Redirect to='/admin' from="/"/>
            <Redirect to='/404'/>
        </Switch>
    </Router>,
    document.getElementById('root')
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
